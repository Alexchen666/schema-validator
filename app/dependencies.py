from app.models.db import SessionLocal


# --- Database ---
def get_db():
    """
    Yields a SQLAlchemy session and guarantees it is closed after the
    request finishes, even if an exception is raised.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
