from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.models import db as models
from app.models.schemas import ValidationRunListResponse, ValidationRunResponse

router = APIRouter(prefix="/history", tags=["history"])


@router.get("/", response_model=ValidationRunListResponse)
def list_runs(
    schema_id: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(models.ValidationRun)

    # Optional filters
    if schema_id:
        query = query.filter_by(schema_id=schema_id)
    if status_filter:
        query = query.filter_by(status=status_filter)

    total = query.count()
    runs = (
        query
        .order_by(models.ValidationRun.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {"runs": runs, "total": total, "limit": limit, "offset": offset}


@router.get("/{run_id}", response_model=ValidationRunResponse)
def get_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(models.ValidationRun).filter_by(id=run_id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Validation run '{run_id}' not found",
        )
    return run


@router.delete("/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(models.ValidationRun).filter_by(id=run_id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Validation run '{run_id}' not found",
        )
    db.delete(run)
    db.commit()
