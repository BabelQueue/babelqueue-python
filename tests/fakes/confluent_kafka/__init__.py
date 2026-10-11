"""In-process stand-in for the slice of confluent_kafka the live tests and KafkaTransport use.

Single partition per topic, per-group committed offsets, cumulative commit, seek, and
position-from-committed on a new consumer — the semantics the transport relies on."""
import threading

_LOCK = threading.RLock()
TOPICS = {}      # topic -> list[Message]
COMMITTED = {}   # (group, topic) -> next offset to read

OFFSET_BEGINNING = -2


class TopicPartition:
    def __init__(self, topic, partition=0, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset


class Message:
    def __init__(self, topic, offset, value, headers, key=None):
        self._t, self._o, self._v, self._h, self._k = topic, offset, value, headers, key

    def key(self): return self._k  # None = null key, b"" = empty key (kept distinct)
    def value(self): return self._v
    def headers(self): return self._h
    def error(self): return None
    def topic(self): return self._t
    def partition(self): return 0
    def offset(self): return self._o


class Producer:
    def __init__(self, conf): self._reports = []

    def produce(self, topic, value=None, headers=None, timestamp=None, on_delivery=None, key=None, **kw):
        with _LOCK:
            log = TOPICS.setdefault(topic, [])  # auto-create, like the real broker default
            log.append(Message(topic, len(log), value, list(headers or []), key))
        if on_delivery is not None:
            self._reports.append(on_delivery)

    def poll(self, t): return 0

    def flush(self, *a):
        # Serve the delivery reports like librdkafka does: from poll()/flush(), all successful.
        reports, self._reports = self._reports, []
        for report in reports:
            report(None, None)
        return 0


class Consumer:
    def __init__(self, conf):
        self.group = conf["group.id"]
        self.topic = None
        self.position = None

    def subscribe(self, topics):
        self.topic = topics[0]
        self.position = COMMITTED.get((self.group, self.topic), 0)

    def assign(self, partitions):
        tp = partitions[0]
        self.topic = tp.topic
        self.position = 0 if tp.offset == OFFSET_BEGINNING else tp.offset

    def poll(self, timeout=None):
        with _LOCK:
            log = TOPICS.get(self.topic, [])
            if self.position < len(log):
                m = log[self.position]
                self.position += 1
                return m
        import time
        time.sleep(min(timeout or 0.05, 0.05))
        return None

    def commit(self, message=None, asynchronous=True):
        with _LOCK:
            key = (self.group, message.topic())
            COMMITTED[key] = max(COMMITTED.get(key, 0), message.offset() + 1)

    def seek(self, tp):
        self.position = tp.offset

    def committed(self, partitions, timeout=None):
        return [TopicPartition(p.topic, p.partition, COMMITTED.get((self.group, p.topic), -1001)) for p in partitions]

    def close(self): pass
