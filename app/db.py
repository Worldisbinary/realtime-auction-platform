"""Engine/session setup and ORM models."""
import os

from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class Auction(Base):
    __tablename__ = "auctions"
    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(16))  # english | dutch | sealed
    title: Mapped[str] = mapped_column(String(200))
    status: Mapped[str] = mapped_column(String(8), default="open")
    start_price: Mapped[int] = mapped_column(Integer)
    current_price: Mapped[int] = mapped_column(Integer, default=0)
    min_increment: Mapped[int] = mapped_column(Integer, default=1)
    floor_price: Mapped[int] = mapped_column(Integer, default=0)
    decrement: Mapped[int] = mapped_column(Integer, default=0)
    interval_seconds: Mapped[float] = mapped_column(Float, default=1.0)
    reserve_price: Mapped[int] = mapped_column(Integer, default=0)
    snipe_window: Mapped[float] = mapped_column(Float, default=0.0)
    extension: Mapped[float] = mapped_column(Float, default=0.0)
    starts_at: Mapped[float] = mapped_column(Float)
    ends_at: Mapped[float] = mapped_column(Float)
    bid_count: Mapped[int] = mapped_column(Integer, default=0)
    leader: Mapped[str | None] = mapped_column(String(64), nullable=True)
    winner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    final_price: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Optimistic-lock token: every state change is UPDATE ... WHERE version = :seen.
    version: Mapped[int] = mapped_column(Integer, default=0)


class Bid(Base):
    __tablename__ = "bids"
    __table_args__ = (
        UniqueConstraint("auction_id", "idem_key", name="uq_bid_idem"),
        Index("ix_bids_auction_version", "auction_id", "version"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    auction_id: Mapped[int] = mapped_column(ForeignKey("auctions.id"))
    bidder: Mapped[str] = mapped_column(String(64))
    amount: Mapped[int] = mapped_column(Integer)
    version: Mapped[int] = mapped_column(Integer)  # auction version this bid produced
    idem_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[float] = mapped_column(Float)


def database_url() -> str:
    return os.environ.get("DATABASE_URL", "sqlite:///./auction.db")


def make_engine(url: str):
    if url.startswith("sqlite"):
        eng = create_engine(url, connect_args={"check_same_thread": False, "timeout": 30})

        @event.listens_for(eng, "connect")
        def _pragmas(conn, _):
            conn.execute("PRAGMA journal_mode=WAL")

        return eng
    return create_engine(url, pool_size=20, max_overflow=40, pool_pre_ping=True)


def make_factory(engine) -> sessionmaker:
    return sessionmaker(engine, expire_on_commit=False)


def init_schema(engine) -> None:
    """Create tables; a Postgres advisory lock keeps two booting instances from racing."""
    with engine.connect() as conn:
        pg = engine.dialect.name == "postgresql"
        if pg:
            conn.execute(text("SELECT pg_advisory_lock(727)"))
        try:
            Base.metadata.create_all(conn)
            conn.commit()
        finally:
            if pg:
                conn.execute(text("SELECT pg_advisory_unlock(727)"))
                conn.commit()
