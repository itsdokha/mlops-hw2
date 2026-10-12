"""Загрузка транзакций из CSV в топик Kafka (одна строка CSV -> одно JSON-сообщение)."""

import argparse
import csv
import json
import os
import sys
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def parse_args():
    parser = argparse.ArgumentParser(description="CSV -> Kafka producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--file", default=os.getenv("CSV_PATH", "data/train.csv"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None, help="отправить только первые N строк")
    return parser.parse_args()


def wait_for_kafka(admin, timeout=120):
    deadline = time.time() + timeout
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except KafkaException as exc:
            if time.time() > deadline:
                raise
            print(f"Kafka is not ready yet: {exc}; retrying...")
            time.sleep(3)


def ensure_topic(admin, topic, partitions):
    if topic in admin.list_topics(timeout=10).topics:
        print(f"Topic '{topic}' already exists")
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    try:
        futures[topic].result()
        print(f"Topic '{topic}' created ({partitions} partitions)")
    except KafkaException as exc:
        # топик мог быть создан параллельно
        print(f"Topic creation: {exc}")


def main():
    args = parse_args()
    if not os.path.exists(args.file):
        sys.exit(f"CSV file not found: {args.file}")

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    wait_for_kafka(admin)
    ensure_topic(admin, args.topic, args.partitions)

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    })

    errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1

    sent = 0
    started = time.time()
    with open(args.file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            payload = json.dumps(row, ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce(args.topic, value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    # локальная очередь переполнена — ждём отправки
                    producer.poll(0.5)
            sent += 1
            if sent % 10000 == 0:
                producer.poll(0)
            if sent % 100000 == 0:
                print(f"sent {sent} messages")
            if args.limit and sent >= args.limit:
                break

    producer.flush()
    print(f"Done: {sent} messages sent to '{args.topic}' in {time.time() - started:.1f}s, errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
