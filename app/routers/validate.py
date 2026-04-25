import io

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.models import db as models
from app.models.schemas import ValidationReportResponse
from app.services.validator import validate
from app.utils.logging import get_logger

router = APIRouter(prefix="/validate", tags=["validate"])
logger = get_logger(__name__)


@router.post(
    "/{schema_id}",
    response_model=ValidationReportResponse,
)
async def validate_file(
    schema_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    # 1. Check schema exists
    schema_record = (
        db.query(models.SchemaConfig).filter_by(id=schema_id).first()
    )
    if not schema_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schema '{schema_id}' not found",
        )

    # 2. Validate file type
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are supported",
        )

    # 3. Parse CSV
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse CSV: {e}",
        )

    # 4. Run validation
    logger.info(
        f"Validating file '{file.filename}' against schema '{schema_id}'"
    )
    report = validate(df, schema_record.storage_path)

    # 5. Persist run to history
    history_record = models.ValidationRun(
        schema_id=schema_id,
        filename=file.filename,
        total_rows=report.total_rows,
        valid_rows=report.valid_rows,
        error_count=report.error_count,
        status=report.status,
    )
    db.add(history_record)
    db.commit()

    return report
