import os
import shutil

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.config import settings
from app.dependencies import get_db
from app.models import db as models
from app.models.schemas import SchemaConfigListResponse, SchemaConfigResponse
from app.services.yaml_parser import YAMLParseError, parse_yaml
from app.utils.logging import get_logger

router = APIRouter(prefix="/schemas", tags=["schemas"])
logger = get_logger(__name__)


@router.post(
    "/",
    response_model=SchemaConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def upload_schema(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    # 1. Validate file type
    if not file.filename.endswith((".yaml", ".yml")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only YAML files are supported",
        )

    # 2. Save file temporarily to parse and validate it
    os.makedirs(settings.schema_storage_path, exist_ok=True)
    storage_path = os.path.join(settings.schema_storage_path, file.filename)

    try:
        with open(storage_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save schema file: {e}",
        )

    # 3. Parse and validate YAML structure — fail fast before persisting
    try:
        parsed = parse_yaml(storage_path)
    except YAMLParseError as e:
        os.remove(storage_path)  # clean up invalid file
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid schema: {e}",
        )

    # 4. Check for duplicate name
    existing = db.query(models.SchemaConfig).filter_by(name=parsed.name).first()
    if existing:
        os.remove(storage_path)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Schema with name '{parsed.name}' already exists. "
            f"Use PUT /{existing.id} to update it.",
        )

    # 5. Persist to DB
    record = models.SchemaConfig(
        name=parsed.name,
        version=parsed.version,
        filename=file.filename,
        storage_path=storage_path,
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    logger.info(
        f"Schema '{parsed.name}' uploaded successfully (id={record.id})"
    )
    return record


@router.get(
    "/",
    response_model=SchemaConfigListResponse,
)
def list_schemas(db: Session = Depends(get_db)):
    schemas = db.query(models.SchemaConfig).all()
    return {"schemas": schemas}


@router.get(
    "/{schema_id}",
    response_model=SchemaConfigResponse,
)
def get_schema(schema_id: str, db: Session = Depends(get_db)):
    record = db.query(models.SchemaConfig).filter_by(id=schema_id).first()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schema '{schema_id}' not found",
        )
    return record


@router.put(
    "/{schema_id}",
    response_model=SchemaConfigResponse,
)
async def update_schema(
    schema_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    # 1. Check schema exists
    record = db.query(models.SchemaConfig).filter_by(id=schema_id).first()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schema '{schema_id}' not found",
        )

    if not file.filename.endswith((".yaml", ".yml")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only YAML files are supported",
        )

    # 2. Save and validate new file
    new_storage_path = os.path.join(settings.schema_storage_path, file.filename)

    try:
        with open(new_storage_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        parsed = parse_yaml(new_storage_path)
    except YAMLParseError as e:
        os.remove(new_storage_path)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid schema: {e}",
        )

    # 3. Remove old file if path changed
    if record.storage_path != new_storage_path:
        if os.path.exists(record.storage_path):
            os.remove(record.storage_path)

    # 4. Update record
    record.name = parsed.name
    record.version = parsed.version
    record.filename = file.filename
    record.storage_path = new_storage_path
    db.commit()
    db.refresh(record)

    logger.info(f"Schema '{schema_id}' updated successfully")
    return record


@router.delete(
    "/{schema_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_schema(schema_id: str, db: Session = Depends(get_db)):
    record = db.query(models.SchemaConfig).filter_by(id=schema_id).first()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schema '{schema_id}' not found",
        )

    # Remove file from disk
    if os.path.exists(record.storage_path):
        os.remove(record.storage_path)

    db.delete(record)
    db.commit()

    logger.info(f"Schema '{schema_id}' deleted")
