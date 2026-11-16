from __future__ import annotations

import threading
import time

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.infra import cache as cache_module
from app.infra import redis_cache
from app.services.analytics_service import AnalyticsService
from app.services.cache_warmup import build_warmup_paths


class FakeRedis:
    """Cliente minimo com a superficie usada pela API (get/set/delete/info/dbsize)."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex:
            self.ttls[key] = ex
        return True

    def delete(self, key):
        self.store.pop(key, None)

    def dbsize(self):
        return len(self.store)

    def info(self, section):
        return {"keyspace_hits": 3, "keyspace_misses": 1, "used_memory": 2 * 1024 * 1024}


@pytest.fixture(autouse=True)
def clean_cache():
    cache_module.ANALYTICS_CACHE.clear()
    with cache_module.CACHE_STATS_LOCK:
        for key in cache_module.CACHE_STATS:
            cache_module.CACHE_STATS[key] = 0
        cache_module.ROUTE_STATS.clear()
    yield
    cache_module.ANALYTICS_CACHE.clear()


@pytest.fixture
def client():
    rows = []
    for sq, (partido, situacao, votos) in enumerate(
        [("AAA", "ELEITO", 100), ("AAA", "ELEITO", 90), ("BBB", "ELEITO", 80), ("BBB", "NÃO ELEITO", 10)], start=1
    ):
        rows.append(
            {
                "ANO_ELEICAO": 2026, "NR_TURNO": 1, "SG_UF": "SP", "NM_UE": "SÃO PAULO",
                "DS_CARGO": "Deputado Federal", "DS_SIT_TOT_TURNO": situacao, "SQ_CANDIDATO": sq,
                "NM_CANDIDATO": f"Candidato {sq}", "SG_PARTIDO": partido,
                "QT_VOTOS_NOMINAIS_VALIDOS": votos, "IDADE": 40 + sq, "DS_COR_RACA": "BRANCA",
                "DS_GENERO": "FEMININO", "DS_OCUPACAO": "ADVOGADO",
            }
        )
    with TestClient(main_module.app) as test_client:
        main_module.service = AnalyticsService(dataframe=pd.DataFrame(rows), default_top_n=20, max_top_n=100)
        yield test_client
    main_module.service = None


CACHED_ROUTES = [
    "/v1/analytics/ranking?metric=eleitos&group_by=partido&ano=2026&cargo=Deputado%20Federal",
    "/v1/analytics/idade?ano=2026",
    "/v1/analytics/cor-raca-comparativo?ano=2026",
    "/v1/analytics/ocupacao-genero?ano=2026",
    "/v1/analytics/vagas-oficiais?group_by=cargo&ano=2026",
    "/v1/analytics/mapa-uf?ano=2026",
    "/v1/analytics/serie-temporal",
]


@pytest.mark.parametrize("path", CACHED_ROUTES)
def test_formerly_uncached_routes_hit_cache_on_second_call(client, path):
    first = client.get(path)
    if first.status_code != 200:
        pytest.skip(f"rota sem dados no fixture: {first.status_code}")
    second = client.get(path)
    assert first.headers["X-Memory-Cache"] == "MISS"
    assert second.headers["X-Memory-Cache"] == "HIT"
    assert second.json() == first.json()


def test_ranking_counts_elected_candidates_once(client):
    body = client.get("/v1/analytics/ranking?metric=eleitos&group_by=partido&ano=2026&cargo=Deputado%20Federal").json()
    assert {item["label"]: item["value"] for item in body["items"]} == {"AAA": 2.0, "BBB": 1.0}


def test_single_flight_runs_cold_query_once_for_concurrent_requests():
    calls = {"n": 0}
    key = "snap:/x:{}"

    def lookup():
        return cache_module.cache_get(key)

    def compute():
        calls["n"] += 1
        time.sleep(0.3)
        cache_module.cache_set(key, 60, {"ok": True})
        return {"ok": True}

    results = []
    barrier = threading.Barrier(10)

    def worker():
        barrier.wait()
        results.append(cache_module.single_flight(key, lookup, compute)[0])

    threads = [threading.Thread(target=worker) for _ in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert calls["n"] == 1
    assert results == [{"ok": True}] * 10
    assert cache_module.CACHE_STATS["single_flight_waits"] >= 1


def test_redis_receives_payloads_larger_than_l1_entry_limit(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)
    monkeypatch.setattr(cache_module.settings, "analytics_cache_max_entry_kb", 1)
    payload = {"items": ["x" * 50] * 100}  # ~5 KB, acima do limite de 1 KB do L1

    stored = cache_module.cache_set("v1:big", 3600, payload)

    assert stored["stored"] is False and stored["reason"] == "entry_too_large"
    assert "v1:big" in fake.store and fake.ttls["v1:big"] == 3600
    cache_module.ANALYTICS_CACHE.clear()
    assert cache_module.cache_get("v1:big") == payload
    assert cache_module.CACHE_STATS["l2_hits"] == 1


def test_redis_lock_makes_second_process_wait_for_the_first(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)
    monkeypatch.setattr(cache_module.settings, "single_flight_wait_seconds", 5.0)
    key = "v1:shared"
    assert redis_cache.redis_lock(f"lock:{key}", 30) is True  # outra replica ja esta calculando

    def other_replica_finishes():
        time.sleep(0.4)
        fake.set(key, b'{"from":"other-replica"}', ex=60)

    threading.Thread(target=other_replica_finishes).start()
    computed = []
    value, was_cached = cache_module.single_flight(
        key, lambda: cache_module.cache_get(key), lambda: computed.append(1) or {"from": "self"}
    )
    assert value == {"from": "other-replica"} and was_cached is True and computed == []


def test_redis_unavailable_falls_back_without_error(monkeypatch):
    class Broken:
        def get(self, *_):
            raise ConnectionError

        def set(self, *_, **__):
            raise ConnectionError

    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: Broken())
    assert cache_module.cache_set("v1:k", 60, {"a": 1})["stored"] is True
    assert cache_module.cache_get("v1:k") == {"a": 1}
    assert redis_cache.redis_lock("lock:k", 30) is None


def test_closed_election_ttl_is_long_and_snapshot_is_part_of_the_key(monkeypatch):
    assert cache_module.MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/ranking"] >= 7 * 24 * 3600
    monkeypatch.setattr(cache_module, "DATA_CACHE_VERSION", "snap-A")
    key_a = cache_module.versioned_cache_key("/v1/analytics/ranking", {"ano": 2026})
    monkeypatch.setattr(cache_module, "DATA_CACHE_VERSION", "snap-B")
    key_b = cache_module.versioned_cache_key("/v1/analytics/ranking", {"ano": 2026})
    assert key_a != key_b and key_a.startswith("snap-A:") and key_b.startswith("snap-B:")


def test_health_cache_reports_snapshot_hit_rate_and_route_times(client, monkeypatch):
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: FakeRedis())
    path = "/v1/analytics/idade?ano=2026"
    client.get(path)
    client.get(path)
    body = client.get("/health/cache").json()
    assert body["status"] == "ok"
    assert body["snapshot"]
    assert body["hits"] >= 1 and body["misses"] >= 1 and 0 < body["hit_rate"] < 1
    route = body["routes"]["/v1/analytics/idade"]
    assert route["calls"] == 2 and route["hit_rate"] == 0.5 and route["avg_ms"] >= 0
    assert body["redis"]["enabled"] is True and body["redis"]["hit_rate"] == 0.75
    assert client.get("/health/cache").headers["Cache-Control"].startswith("no-store")
    assert set(client.get("/health").json()) == {"status", "data_loaded", "version"}


def test_warmup_paths_cover_panels_without_duplicates():
    paths = list(build_warmup_paths([2026]))
    assert len(paths) == len(set(paths))
    assert "/v1/analytics/ranking?ano=2026&cargo=Senador&turno=1&group_by=partido&metric=eleitos&top_n=100" in paths
    assert any(p.startswith("/v1/analytics/distribuicao") and "group_by=ocupacao" in p for p in paths)
    assert any("uf=SP" in p and "cargo=Deputado+Federal" in p for p in paths)
    assert not any("turno=2" in p and "Senador" in p for p in paths)


def test_internal_warmup_token_bypasses_ip_rate_limit(client, monkeypatch):
    from app.services.cache_warmup import WARMUP_HEADER, WARMUP_TOKEN

    monkeypatch.setattr(main_module.settings, "rate_limit_enabled", True)
    monkeypatch.setattr(main_module.settings, "rate_limit_max_requests_per_ip", 3)
    ip = {"X-Forwarded-For": "203.0.113.77"}
    codes = [client.get("/v1/analytics/idade?ano=2026", headers=ip).status_code for _ in range(5)]
    assert codes[:3] == [200, 200, 200] and set(codes[3:]) == {429}
    warm = {**ip, WARMUP_HEADER: WARMUP_TOKEN}
    assert all(client.get("/v1/analytics/idade?ano=2026", headers=warm).status_code == 200 for _ in range(10))
    wrong = {**ip, WARMUP_HEADER: "chute"}
    assert client.get("/v1/analytics/idade?ano=2026", headers=wrong).status_code == 429


def test_warmup_retries_429_and_counts_it(monkeypatch):
    import urllib.error

    from app.services import cache_warmup

    calls = {"n": 0}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise urllib.error.HTTPError(request.full_url, 429, "Too Many", {"Retry-After": "0"}, None)
        return Resp()

    monkeypatch.setattr(cache_warmup.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(cache_warmup.time, "sleep", lambda _: None)
    status = {"failed": 0, "throttled": 0}
    cache_warmup._fetch("http://x/v1/analytics/idade", 1.0, "tok", status)
    assert calls["n"] == 3 and status == {"failed": 0, "throttled": 2}

    calls["n"] = -100  # nunca sai do 429: esgota as repeticoes e conta como falha
    status = {"failed": 0, "throttled": 0}
    cache_warmup._fetch("http://x/v1/analytics/idade", 1.0, None, status)
    assert status["failed"] == 1 and status["throttled"] == cache_warmup.MAX_THROTTLE_RETRIES


def test_redis_lock_is_released_after_compute_even_when_it_fails(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)
    key = "v1:release"
    ok, _ = cache_module.single_flight(key, lambda: None, lambda: {"v": 1})
    assert ok == {"v": 1} and f"lock:{key}" not in fake.store
    with pytest.raises(RuntimeError):
        cache_module.single_flight(key, lambda: None, lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert f"lock:{key}" not in fake.store


def test_l1_is_checked_before_redis_and_redis_hits_are_promoted(monkeypatch):
    fake = FakeRedis()
    reads = {"n": 0}
    original_get = fake.get
    fake.get = lambda key: reads.__setitem__("n", reads["n"] + 1) or original_get(key)
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)
    cache_module.cache_set("v1:k", 3600, {"a": 1})
    assert cache_module.cache_get("v1:k") == {"a": 1} and reads["n"] == 0  # L1 resolve sem tocar no Redis
    cache_module.ANALYTICS_CACHE.clear()
    assert cache_module.cache_get("v1:k") == {"a": 1} and reads["n"] == 1  # veio do Redis
    assert cache_module.cache_get("v1:k") == {"a": 1} and reads["n"] == 1  # promovido: segunda leitura no L1
    assert cache_module.CACHE_STATS["l2_hits"] == 1 and cache_module.CACHE_STATS["l1_hits"] == 2
