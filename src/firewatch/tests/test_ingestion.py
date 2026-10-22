"""Tests for the ingestion service (fake producer, no Kafka needed)."""

from types import SimpleNamespace

from firewatch.services.ingestion import publish_ingest, run_ingestion


class FakeFuture:
    def __init__(self, metadata):
        self._metadata = metadata

    def get(self, timeout=None):
        return self._metadata


class FakeProducer:
    """Minimal stand-in for kafka.KafkaProducer."""

    def __init__(self):
        self.sent: list[tuple[str, dict]] = []
        self.flushed = False
        self.closed = False

    def send(self, topic, value):
        self.sent.append((topic, value))
        offset = len(self.sent) - 1
        return FakeFuture(SimpleNamespace(topic=topic, partition=0, offset=offset))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def test_publish_ingest_sends_single_record(firms_sample):
    producer = FakeProducer()
    publish_ingest(producer, firms_sample[0], topic="test-topic")

    assert len(producer.sent) == 1
    topic, record = producer.sent[0]
    assert topic == "test-topic"
    assert record == firms_sample[0]


def test_run_ingestion_publishes_all_records(firms_sample):
    producer = FakeProducer()
    count = run_ingestion(firms_sample, producer, topic="test-topic")

    assert count == 2
    assert len(producer.sent) == 2
    assert producer.flushed is True
    assert {r["latitude"] for _, r in producer.sent} == {19.5, -3.1}


def test_run_ingestion_empty_batch():
    producer = FakeProducer()
    count = run_ingestion([], producer, topic="test-topic")

    assert count == 0
    assert producer.sent == []
