import threading
import time

import pytest

from faketickets.engine.dispatcher import Dispatcher, Op
from faketickets.engine.histogram import Histogram, bucket_index, bucket_upper
from faketickets.engine.ids import INT_MAX, IdAllocator, IdSpaceExhausted
from faketickets.engine.ratelimit import RateLimiter
from faketickets.engine.stats import Recorder
from faketickets.worker import share


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def drive(lim, clock, seconds, step=0.001, max_n=10_000):
    granted = 0
    end = clock.t + seconds
    while clock.t < end:
        granted += lim.acquire(max_n, max_wait=step)
    return granted


@pytest.mark.parametrize("rate", [1, 10, 250, 1000, 20_000, 100_000])
def test_rate_limiter_is_accurate(rate):
    clock = FakeClock()
    lim = RateLimiter(rate, clock=clock, sleep=clock.sleep)
    got = drive(lim, clock, 10.0)
    assert abs(got - rate * 10) <= max(2, rate * 10 * 0.01)


def test_rate_change_takes_effect_without_burst():
    clock = FakeClock()
    lim = RateLimiter(1000, clock=clock, sleep=clock.sleep)
    drive(lim, clock, 2.0)
    lim.set_rate(100)
    got = drive(lim, clock, 5.0)
    assert 480 <= got <= 520


def test_debt_is_capped_after_stall():
    """A consumer that stalls (driver back-pressure) must not get a catch-up burst."""
    clock = FakeClock()
    lim = RateLimiter(1000, clock=clock, sleep=clock.sleep, burst_s=0.1)
    clock.t += 30  # 30 s stall = 30 000 "owed" permits
    n, _ = lim.try_acquire(1_000_000)
    assert n <= 100


def test_unlimited_rate_grants_everything():
    lim = RateLimiter(0)
    assert lim.try_acquire(512) == (512, 0.0)


def test_real_clock_rate():
    lim = RateLimiter(2000)
    t0 = time.monotonic()
    n = 0
    while time.monotonic() - t0 < 1.0:
        n += lim.acquire(256)
    assert 1800 <= n <= 2200


def test_histogram_percentiles():
    h = Histogram()
    for ms in range(1, 101):  # 1..100 ms uniform
        h.record(ms / 1000)
    assert h.percentile(50) == pytest.approx(0.050, rel=0.1)
    assert h.percentile(99) == pytest.approx(0.099, rel=0.1)
    assert h.max_s == pytest.approx(0.100)
    h2 = Histogram()
    h2.merge_counts({str(k): v for k, v in h.counts.items()}, h.max_s)
    assert h2.percentile(95) == h.percentile(95)


def test_histogram_buckets_monotonic():
    prev = -1
    for s in (1e-5, 1e-4, 1e-3, 0.01, 0.1, 1, 10):
        i = bucket_index(s)
        assert i >= prev
        assert bucket_upper(i) >= s * 0.999 or i == 0
        prev = i


def test_id_allocator_overflow_and_stride():
    a = IdAllocator(INT_MAX - 2, limit=INT_MAX)
    assert [a.next() for _ in range(3)] == [INT_MAX - 2, INT_MAX - 1, INT_MAX]
    with pytest.raises(IdSpaceExhausted):
        a.next()
    assert a.high_water == INT_MAX + 1
    b = IdAllocator(100, offset=1, stride=3)
    assert [b.next() for _ in range(3)] == [101, 104, 107]


def test_share_splits_counts():
    assert [share(10, i, 3) for i in range(3)] == [4, 3, 3]
    assert share(0, 1, 4) == 0


class _Fut:
    def __init__(self, ok, delay):
        self.ok, self.delay = ok, delay

    def add_callbacks(self, cb, eb, callback_args=(), errback_args=()):
        def fire():
            time.sleep(self.delay)
            if self.ok:
                cb(["row"], *callback_args)
            else:
                eb(RuntimeError("nope"), *errback_args)

        threading.Thread(target=fire, daemon=True).start()


class _Sess:
    def __init__(self):
        self.peak = 0
        self.n = 0

    def execute_async(self, stmt, args, timeout=None):
        self.n += 1
        return _Fut(ok=(args[0] % 10 != 0), delay=0.01)


def test_dispatcher_respects_inflight_cap_and_counts():
    rec = Recorder()
    sess = _Sess()
    d = Dispatcher(sess, rec, concurrency=5)
    peak = 0
    hits = []
    for i in range(1, 101):
        while d.free_slots(0.5) == 0:
            pass
        d.submit(Op(None, (i,), "write", on_rows=lambda rows: hits.append(rows)))
        peak = max(peak, d.inflight)
    assert d.drain(5)
    assert peak <= 5
    snap = rec.drain()
    assert snap["ok"]["write"] == 90 and snap["err"]["write"] == 10
    assert "RuntimeError" in snap["errtypes"]
    assert len(hits) == 90
