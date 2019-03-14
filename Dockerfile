FROM python:3.6

RUN pip install alpaca-trade-api structlog
RUN mkdir -p /work

WORKDIR /work/hft
ADD .  /work/hft
