from __future__ import annotations
import asyncio, time
from collections import defaultdict

class DomainRateLimiter:
    def __init__(self, delay: float = 0.35):
        self.delay = delay
        self._last = defaultdict(float)
        self._locks = defaultdict(asyncio.Lock)

    async def wait(self, domain: str):
        async with self._locks[domain]:
            elapsed = time.time() - self._last[domain]
            if elapsed < self.delay:
                await asyncio.sleep(self.delay - elapsed)
            self._last[domain] = time.time()
