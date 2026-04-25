import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from app.config import settings

Base = declarative_base()


def generate_uuid() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --- Tables ---
class SchemaConfig(Base):
    __tablename__ = "schema_configs"

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False, unique=True, index=True)
    version = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    storage_path = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )

    runs = relationship(
        "ValidationRun",
        back_populates="schema",
        cascade="all, delete-orphan",  # deleting a schema deletes its run history
    )


class ValidationRun(Base):
    __tablename__ = "validation_runs"

    id = Column(String, primary_key=True, default=generate_uuid)
    schema_id = Column(
        String, ForeignKey("schema_configs.id"), nullable=False, index=True
    )
    filename = Column(String, nullable=False)
    status = Column(String, nullable=False)  # passed | failed | error
    total_rows = Column(Integer, nullable=False)
    valid_rows = Column(Integer, nullable=False)
    error_count = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

    schema = relationship("SchemaConfig", back_populates="runs")


# --- Engine and session ---

engine = create_engine(
    settings.database_url,
    # Required for SQLite only — safe to leave for other DBs
    connect_args={"check_same_thread": False}
    if settings.database_url.startswith("sqlite")
    else {},
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def create_tables():
    """Creates all tables. Called once at app startup."""
    Base.metadata.create_all(bind=engine)
