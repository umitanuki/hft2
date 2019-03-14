import argparse
import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
import structlog


def modify_msg(_, __, msg):
    t = pd.Timestamp.now(tz='America/New_York').isoformat()
    event = msg.pop('event', None)
    s = msg.pop('s', None)
    d = dict(t=t, event=event, s=s, **msg)
    if not event:
        del d['event']
    if not s:
        del d['s']
    return d


structlog.configure(
    processors=[
        modify_msg,
        structlog.processors.JSONRenderer()])
slog = structlog.get_logger()


class Quote():
    """
    We use Quote objects to represent the bid/ask spread. When we encounter a
    'level change', a move of exactly 1 penny, we may attempt to make one
    trade. Whether or not the trade is successfully filled, we do not submit
    another trade until we see another level change.

    Note: Only moves of 1 penny are considered eligible because larger moves
    could potentially indicate some newsworthy event for the stock, which this
    algorithm is not tuned to trade.
    """

    def __init__(self, symbol):
        self._symbol = symbol
        self.prev_bid = 0
        self.prev_ask = 0
        self.prev_spread = 0
        self.bid = 0
        self.ask = 0
        self.bid_size = 0
        self.ask_size = 0
        self.spread = 0
        self.traded = True
        self.level_ct = 1
        self.time = 0

    def reset(self):
        # Called when a level change happens
        self.traded = False
        self.level_ct += 1

    def update(self, data):
        # Update bid and ask sizes and timestamp
        self.bid_size = data.bidsize
        self.ask_size = data.asksize

        # Check if there has been a level change
        if (
            self.bid != data.bidprice
            and self.ask != data.askprice
            and round(data.askprice - data.bidprice, 2) == .01
        ):
            # Update bids and asks and time of level change
            self.prev_bid = self.bid
            self.prev_ask = self.ask
            self.bid = data.bidprice
            self.ask = data.askprice
            self.time = data.timestamp
            # Update spreads
            self.prev_spread = round(self.prev_ask - self.prev_bid, 3)
            self.spread = round(self.ask - self.bid, 3)
            slog.msg('level change',
                     s=self._symbol,
                     prev_bid=self.prev_bid,
                     prev_ask=self.prev_ask,
                     prev_spread=self.prev_spread,
                     bid=self.bid,
                     ask=self.ask,
                     spread=self.spread,
                     )
            # If change is from one penny spread level to a different penny
            # spread level, then initialize for new level (reset stale vars)
            if self.prev_spread == 0.01:
                self.reset()


class Position():
    """
    The position object is used to track how many shares we have. We need to
    keep track of this so our position size doesn't inflate beyond the level
    we're willing to trade with. Because orders may sometimes be partially
    filled, we need to keep track of how many shares are "pending" a buy or
    sell as well as how many have been filled into our account.
    """

    def __init__(self, symbol):
        self._symbol = symbol
        self.orders_filled_amount = {}
        self.pending_buy_shares = 0
        self.pending_sell_shares = 0
        self.total_shares = 0

    def update_pending_buy_shares(self, quantity):
        self.pending_buy_shares += quantity

    def update_pending_sell_shares(self, quantity):
        self.pending_sell_shares += quantity

    def update_filled_amount(self, order_id, new_amount, side):
        old_amount = self.orders_filled_amount[order_id]
        if new_amount > old_amount:
            if side == 'buy':
                self.update_pending_buy_shares(old_amount - new_amount)
                self.update_total_shares(new_amount - old_amount)
            else:
                self.update_pending_sell_shares(old_amount - new_amount)
                self.update_total_shares(old_amount - new_amount)
            self.orders_filled_amount[order_id] = new_amount

    def remove_pending_order(self, order_id, side, unit):
        old_amount = self.orders_filled_amount[order_id]
        if side == 'buy':
            self.update_pending_buy_shares(old_amount - unit)
        else:
            self.update_pending_sell_shares(old_amount - unit)
        del self.orders_filled_amount[order_id]

    def update_total_shares(self, quantity):
        self.total_shares += quantity

    def sync(self, position, open_orders):
        if position is None:
            self.total_shares = 0
        else:
            self.total_shares = position.qty

        if not open_orders:
            self.pending_buy_shares = 0
            self.pending_sell_shares = 0
        else:
            self.pending_buy_shares = sum(
                [o.qty - o.filled_qty for o in open_orders if o.side == 'buy'])
            self.pending_sell_shares = sum(
                [o.qty - o.filled_qty for o in open_orders if o.side != 'buy'])


def print_status(positions):
    df = pd.DataFrame({symbol: {'pendig_buy': p.pending_buy_shares,
                                'pending_sell': p.pending_sell_shares,
                                'total_shares': p.total_shares} for symbol,
                       p in positions.items()})
    print(df.T)


def setup(api, conn, symbols, unit, max_shares):

    quotes = {}
    positions = {}
    posdata = {pos.symbol: pos for pos in api.list_positions()}
    open_orders = api.list_orders()
    for symbol in symbols:
        quotes[symbol] = Quote(symbol)
        positions[symbol] = Position(symbol)
        positions[symbol].sync(posdata.get(symbol),
                               [o for o in open_orders if o.symbol == symbol])

    print_status(positions)

    # Define our message handling
    @conn.on(r'Q\..*')
    async def on_quote(conn, channel, data):
        slog.msg('Q', s=channel[2:], **data._raw)
        # Quote update received
        quote = quotes[channel[2:]]
        quote.update(data)

    @conn.on(r'T\..*')
    async def on_trade(conn, channel, data):
        slog.msg('T', s=channel[2:], **data._raw)
        t = pd.Timestamp.now(tz='America/New_York')
        if not(
                0 <= t.dayofweek <= 5 and
                t.time() >= pd.Timestamp('09:40').time() and
                t.time() <= pd.Timestamp('12:40').time()):
            return
        symbol = channel[2:]
        quote = quotes[symbol]
        if quote.traded:
            slog.msg('quote.traded', s=symbol)
            return

        position = positions[symbol]

        # We've received a trade and might be ready to follow it
        if (
            data.timestamp <= (
                quote.time + pd.Timedelta(np.timedelta64(5, 'ms'))
            )
        ):
            # The trade came too close to the quote update
            # and may have been for the previous level
            return
        if data.size >= 100:
            # The trade was large enough to follow, so we check to see if
            # we're ready to trade. We also check to see that the
            # bid vs ask quantities (order book imbalance) indicate
            # a movement in that direction. We also want to be sure that
            # we're not buying or selling more than we should.
            if (
                data.price == quote.ask
                and quote.bid_size > (quote.ask_size * 1.8)
                and (
                    position.total_shares + position.pending_buy_shares
                ) < max_shares - unit
            ):
                # Everything looks right, so we submit our buy at the ask
                try:
                    o = api.submit_order(
                        symbol=symbol, qty=unit, side='buy',
                        type='limit', time_in_force='day',
                        limit_price=str(quote.ask)
                    )
                    # Approximate an IOC order by immediately cancelling
                    api.cancel_order(o.id)
                    position.update_pending_buy_shares(unit)
                    position.orders_filled_amount[o.id] = 0
                    slog.msg('buy at', s=symbol, ask=quote.ask,
                             total_shares=position.total_shares,
                             pending_buy=position.pending_buy_shares)
                    quote.traded = True
                except Exception as e:
                    slog.msg('error on buy', s=symbol, error=str(e))
            elif (
                data.price == quote.bid
                and quote.ask_size > (quote.bid_size * 1.8)
                and (
                    position.total_shares - position.pending_sell_shares
                ) >= unit
            ):
                # Everything looks right, so we submit our sell at the bid
                try:
                    o = api.submit_order(
                        symbol=symbol, qty=unit, side='sell',
                        type='limit', time_in_force='day',
                        limit_price=str(quote.bid)
                    )
                    # Approximate an IOC order by immediately cancelling
                    api.cancel_order(o.id)
                    position.update_pending_sell_shares(unit)
                    position.orders_filled_amount[o.id] = 0
                    slog.msg('sell at', s=symbol, bid=quote.bid,
                             total_shares=position.total_shares,
                             pending_sell=position.pending_sell_shares)
                    quote.traded = True
                except Exception as e:
                    slog.msg('error on sell', s=symbol, error=str(e))

    @conn.on(r'trade_updates')
    async def on_trade_updates(conn, channel, data):
        # We got an update on one of the orders we submitted. We need to
        # update our position with the new information.
        event = data.event
        symbol = data.order['symbol']
        position = positions.get(symbol)
        if position is None:
            slog.msg('position not found', s=symbol)
        if event == 'fill':
            slog.msg('filled', s=symbol, **data.order)
            old_amount = position.orders_filled_amount[data.order['id']]
            new_amount = int(data.order['filled_qty'])
            if data.order['side'] == 'buy':
                position.update_total_shares(
                    new_amount - old_amount
                )
            else:
                position.update_total_shares(
                    old_amount - new_amount
                )
            position.remove_pending_order(
                data.order['id'], data.order['side'], unit,
            )
        elif event == 'partial_fill':
            slog.msg('partially_filled', s=symbol, **data.order)
            position.update_filled_amount(
                data.order['id'], int(data.order['filled_qty']),
                data.order['side']
            )
        elif event == 'canceled' or event == 'rejected':
            position.remove_pending_order(
                data.order['id'], data.order['side'], unit,
            )


def run(args):
    symbols = [s.upper() for s in args.symbols.split(',')]
    max_shares = args.quantity
    opts = {}
    if args.key_id:
        opts['key_id'] = args.key_id
    if args.secret_key:
        opts['secret_key'] = args.secret_key
    if args.base_url:
        opts['base_url'] = args.base_url
    elif 'key_id' in opts and opts['key_id'].startswith('PK'):
        opts['base_url'] = 'https://paper-api.alpaca.markets'
    # Create an API object which can be used to submit orders, etc.
    api = tradeapi.REST(**opts)

    qc = ['Q.' + symbol for symbol in symbols]
    tc = ['T.' + symbol for symbol in symbols]
    unit = args.unit

    # Establish streaming connection
    conn = tradeapi.StreamConn(**opts)

    setup(api, conn, symbols, unit, max_shares)

    conn.run(['trade_updates'] + tc + qc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--symbols', type=str, default='SNAP',
        help='Comman-separated symbols you want to trade.'
    )
    parser.add_argument(
        '--unit', type=int, default=100,
        help='trade unit.'
    )
    parser.add_argument(
        '--quantity', type=int, default=500,
        help='Maximum number of shares to hold at once. Minimum 100.'
    )
    parser.add_argument(
        '--key-id', type=str, default=None,
        help='API key ID',
    )
    parser.add_argument(
        '--secret-key', type=str, default=None,
        help='API secret key',
    )
    parser.add_argument(
        '--base-url', type=str, default=None,
        help='set https://paper-api.alpaca.markets if paper trading',
    )
    args = parser.parse_args()
    assert args.quantity >= 100

    run(args)
