import os

import pytest

from app.db import Base, make_engine, make_factory


@pytest.fixture
def engine(tmp_path):
    url = os.environ.get("DATABASE_URL") or f"sqlite:///{tmp_path / 't.db'}"
    eng = make_engine(url)
    Base.metadata.drop_all(eng)
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)
    eng.dispose()


@pytest.fixture
def factory(engine):
    return make_factory(engine)


@pytest.fixture
def db_url(engine):
    return engine.url.render_as_string(hide_password=False)


@pytest.fixture
def redis_factory():
    """Real Redis when REDIS_URL is set (CI), otherwise an in-process fakeredis."""
    url = os.environ.get("REDIS_URL")
    if url:
        import redis
        import redis.asyncio as aioredis

        redis.Redis.from_url(url).flushdb()
        return lambda: aioredis.from_url(url, decode_responses=True)
    import fakeredis

    server = fakeredis.FakeServer()
    return lambda: fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
