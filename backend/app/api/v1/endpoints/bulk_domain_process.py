"""Bulk Domain Processor endpoints — standalone CSV Champion-model runner.

Uploads a CSV of domains, runs the current site-config champion model against
each, and streams back a CSV whose columns align with the production import
schema (see `CSV_OUTPUT_FIELDS` in `app.services.bulk_domain_processor`).
"""

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import PlainTextResponse

from app.services.bulk_domain_processor import (
    CSV_OUTPUT_FIELDS,
    DEFAULT_CONFIDENCE_THRESHOLD,
    build_output_csv,
    parse_input_csv,
    process_domains,
)

router = APIRouter()


@router.get("/schema")
async def schema():
    """Return the CSV column schema so the frontend can show a column preview."""
    return {
        "columns": CSV_OUTPUT_FIELDS,
        "default_confidence_threshold": DEFAULT_CONFIDENCE_THRESHOLD,
    }


@router.post("/run", response_class=PlainTextResponse)
async def run(
    file: UploadFile = File(...),
    confidence_threshold: float = Query(DEFAULT_CONFIDENCE_THRESHOLD, ge=0.0, le=1.0),
):
    """Synchronous run: upload CSV -> parse -> run champion -> return CSV."""
    try:
        raw = await file.read()
        csv_text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Uploaded file is not valid UTF-8")

    try:
        domains = parse_input_csv(csv_text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    results = await process_domains(domains, confidence_threshold=confidence_threshold)
    output = build_output_csv(results, confidence_threshold=confidence_threshold)
    return PlainTextResponse(
        content=output,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="bulk_domain_selectors.csv"'},
    )
