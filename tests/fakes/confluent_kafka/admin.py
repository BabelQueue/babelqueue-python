import concurrent.futures
from . import TOPICS, _LOCK


class NewTopic:
    def __init__(self, name, **kw): self.name = name


class AdminClient:
    def __init__(self, conf): pass

    def create_topics(self, topics):
        out = {}
        for t in topics:
            with _LOCK:
                TOPICS.setdefault(t.name, [])
            f = concurrent.futures.Future()
            f.set_result(None)
            out[t.name] = f
        return out
