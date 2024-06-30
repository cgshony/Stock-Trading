import json
from typing import List
from datetime import datetime
from alpaca_trade_api import REST
from alpaca_trade_api.common import URL
from alpaca.common import Sort
from kafka import KafkaProducer

from alpaca_config.keys import config
from utils import get_sentiment


def create_kafka_producer(brokers: List[str]) -> KafkaProducer:
    """
    Initialize and return a Kafka producer.
    """
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


def fetch_and_send_news(
        kafka_producer: KafkaProducer,
        start_date: str,
        end_date: str,
        symbols: List[str],
        topic: str
    ) -> None:
    """
    Fetch historical news from Alpaca API and send it to the specified Kafka topic.
    """
    key_id = config['key_id']
    secret_key = config['secret_key']
    base_url = config['base_url']

    api = REST(key_id=key_id,
               secret_key=secret_key,
               base_url=URL(base_url))

    for symbol in symbols:
        news_items = api.get_news(
            symbol=symbol,
            start=start_date,
            end=end_date,
            limit=50,
            sort=Sort.ASC,
            include_content=False,
        )

        print(news_items)

        for i, row in enumerate(news_items):
            article = row._raw
            should_proceed = any(term in article['headline'] for term in symbols)
            if not should_proceed:
                continue

            timestamp_ms = int(row.created_at.timestamp() * 1000)
            timestamp = datetime.fromtimestamp(row.created_at.timestamp())

            article['timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            article['timestamp_ms'] = timestamp_ms
            article['data_provider'] = 'alpaca'
            article['sentiment'] = get_sentiment(article['headline'])
            article.pop('symbols')
            article['symbol'] = symbol

            try:
                future = kafka_producer.send(
                    topic=topic,
                    key=symbol,
                    value=article,
                    timestamp_ms=timestamp_ms
                )
                _ = future.get(timeout=10)
                print(f'Sent {i+1} articles to {topic}')
            except Exception as e:
                print(f'Failed to send article: {article}')
                print(e)


if __name__ == '__main__':
    kafka_producer = create_kafka_producer(config['redpanda_brokers'])
    fetch_and_send_news(
        kafka_producer=kafka_producer,
        topic='market-news',
        start_date='2024-01-01',
        end_date='2024-06-08',
        symbols=['NVDA', 'Nvidia']
    )
