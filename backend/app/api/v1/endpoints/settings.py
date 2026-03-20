"""Settings management endpoints."""
import csv
import io
import uuid as _uuid
from typing import Optional
from fastapi import APIRouter, Depends, Query, UploadFile, File, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.base import get_db
from app.models.settings import WordFilter, SystemSetting

router = APIRouter()

SUPPORTED_MARKETS = ["AU", "NZ", "MY", "PH", "ID", "SG", "TH", "HK"]


# ── System settings (markets, discovery sources, schedule) ─────────────────

@router.get("/system/{key}")
async def get_setting(key: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(SystemSetting, key)
    if not row:
        raise HTTPException(404, f"Setting '{key}' not found")
    return row.value

@router.put("/system/{key}")
async def update_setting(key: str, payload: dict, db: AsyncSession = Depends(get_db)):
    row = await db.get(SystemSetting, key)
    if row:
        row.value = payload
    else:
        db.add(SystemSetting(key=key, value=payload))
    await db.commit()
    return {"key": key, "value": payload}


# ── Word filters (bad words + scam words) ─────────────────────────────────

@router.get("/word-filters")
async def list_word_filters(
    filter_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(WordFilter).order_by(WordFilter.created_at.desc())
    if filter_type:
        q = q.where(WordFilter.filter_type == filter_type)
    if search:
        q = q.where(WordFilter.word.ilike(f"%{search}%"))
    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    rows = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))
    items = [
        {
            "id": str(r.id),
            "word": r.word,
            "filter_type": r.filter_type,
            "markets": r.markets,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]
    return {"items": items, "total": total, "page": page, "page_size": page_size}

@router.post("/word-filters", status_code=201)
async def create_word_filter(payload: dict, db: AsyncSession = Depends(get_db)):
    word = payload.get("word", "").strip()
    filter_type = payload.get("filter_type", "bad_word")
    markets = [m for m in payload.get("markets", []) if m in SUPPORTED_MARKETS]
    if not word:
        raise HTTPException(400, "word is required")
    if filter_type not in ("bad_word", "scam_word"):
        raise HTTPException(400, "filter_type must be 'bad_word' or 'scam_word'")
    row = WordFilter(word=word, filter_type=filter_type, markets=markets or SUPPORTED_MARKETS)
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"id": str(row.id), "word": row.word, "filter_type": row.filter_type, "markets": row.markets}

@router.put("/word-filters/{filter_id}")
async def update_word_filter(filter_id: str, payload: dict, db: AsyncSession = Depends(get_db)):
    row = await db.get(WordFilter, _uuid.UUID(filter_id))
    if not row:
        raise HTTPException(404, "Not found")
    if "word" in payload:
        row.word = payload["word"].strip()
    if "markets" in payload:
        row.markets = [m for m in payload["markets"] if m in SUPPORTED_MARKETS]
    await db.commit()
    return {"id": str(row.id), "word": row.word, "filter_type": row.filter_type, "markets": row.markets}

@router.delete("/word-filters/{filter_id}", status_code=204)
async def delete_word_filter(filter_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(WordFilter, _uuid.UUID(filter_id))
    if not row:
        raise HTTPException(404, "Not found")
    await db.delete(row)
    await db.commit()

@router.post("/word-filters/import")
async def import_word_filters(
    filter_type: str = Query(..., description="bad_word or scam_word"),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Import word filters from CSV. First column = word, columns 2-9 = AU,NZ,MY,PH,ID,SG,TH,HK (0/1 or true/false)."""
    if filter_type not in ("bad_word", "scam_word"):
        raise HTTPException(400, "filter_type must be 'bad_word' or 'scam_word'")

    content = await file.read()
    text = content.decode("utf-8-sig")  # handle BOM
    reader = csv.DictReader(io.StringIO(text))

    added = 0
    skipped = 0
    for row in reader:
        word = list(row.values())[0].strip() if row else ""
        if not word:
            continue
        # Parse market flags
        markets = []
        for mkt in SUPPORTED_MARKETS:
            val = row.get(mkt, row.get(mkt.lower(), "0"))
            if str(val).strip().lower() in ("1", "true", "yes"):
                markets.append(mkt)
        if not markets:
            markets = SUPPORTED_MARKETS  # Default to all if no market specified

        wf = WordFilter(word=word, filter_type=filter_type, markets=markets)
        db.add(wf)
        added += 1

    await db.commit()
    return {"imported": added, "skipped": skipped}
