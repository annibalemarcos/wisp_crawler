from utils.hashing import url_fingerprint
class Dedupe:
    def __init__(self):
        self.seen = set()
    def add(self, url: str) -> bool:
        fp = url_fingerprint(url)
        if fp in self.seen: return False
        self.seen.add(fp); return True
