FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir python-binance pandas websocket-client

EXPOSE 8080

CMD ["python", "trading_bot.py"]
