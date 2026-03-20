"""
Seed bad words into the word_filters table via the API.

Run from the repo root:
    docker exec -it jobharvest-api python /app/scripts/seed_bad_words.py
Or directly:
    cd backend && python scripts/seed_bad_words.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.profanity_wordlist import PROFANITY_WORDLIST


SCAM_PHRASES = [
    "get rich quick", "get rich fast", "make money fast", "make money online",
    "earn from home", "work from home earn", "unlimited income",
    "unlimited earning potential", "unlimited earnings", "passive income opportunity",
    "be your own boss", "boss babe", "girl boss", "financial freedom",
    "financial independence opportunity", "six figure income", "six-figure income",
    "residual income", "mlm", "multi level marketing", "multi-level marketing",
    "multilevel marketing", "pyramid scheme", "pyramid selling", "network marketing",
    "direct sales opportunity", "direct selling", "independent distributor",
    "brand ambassador opportunity", "recruitment fee", "processing fee",
    "training fee", "starter kit fee", "starter kit purchase", "buy your own stock",
    "purchase inventory", "no experience required earn", "no experience needed earn",
    "earn while you sleep", "earn while sleeping", "easy money", "quick money",
    "guaranteed income", "guaranteed salary", "guaranteed earnings", "guaranteed profit",
    "wire transfer required", "send money first", "advance fee", "investment required",
    "upfront payment", "upfront investment", "limited spots available",
    "limited opportunity", "once in a lifetime opportunity", "exclusive opportunity",
    "ground floor opportunity", "join our team earn", "build your own team",
    "recruit your friends", "recruit family members", "downline", "upline",
    "commission only", "100% commission", "performance only pay", "no base salary",
    "send cv to gmail", "send cv to yahoo", "send cv to hotmail",
    "whatsapp to apply", "apply via whatsapp", "no interview required",
    "job guaranteed", "immediate start guaranteed", "work your own hours earn",
    "set your own hours", "be your own manager",
    # Market-specific scam signals
    "walang puhunan", "kumita agad", "online selling",  # PH
    "kerja sampingan", "kerja online", "penghasilan tambahan", "bisnis online",
    "tanpa modal", "modal kecil", "untung besar", "rekrut downline",  # ID
    "raiid phiset", "khommishan", "samakr wandee",  # TH
]

# Scam words that should go to scam_word filter_type
SCAM_TERMS_SET = set(SCAM_PHRASES)


async def seed():
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy import select, func, text
    from app.core.config import settings
    from app.models.settings import WordFilter

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    ALL_MARKETS = ["AU", "NZ", "SG", "MY", "HK", "PH", "ID", "TH"]

    inserted_bad = 0
    inserted_scam = 0
    skipped = 0

    async with Session() as db:
        # Get existing words to avoid duplicates
        existing_result = await db.execute(select(WordFilter.word, WordFilter.filter_type))
        existing = {(row.word.lower(), row.filter_type) for row in existing_result}

        batch = []

        for market_key, terms in PROFANITY_WORDLIST.items():
            if market_key == "all":
                markets = ALL_MARKETS
            else:
                markets = [market_key]

            for term in terms:
                word = term.lower().strip()
                if not word:
                    continue

                # Determine filter type
                filter_type = "scam_word" if word in SCAM_TERMS_SET else "bad_word"

                if (word, filter_type) in existing:
                    skipped += 1
                    continue

                existing.add((word, filter_type))
                batch.append(WordFilter(word=word, filter_type=filter_type, markets=markets))

                if filter_type == "bad_word":
                    inserted_bad += 1
                else:
                    inserted_scam += 1

        if batch:
            db.add_all(batch)
            await db.commit()

    print(f"Done. Inserted {inserted_bad} bad words, {inserted_scam} scam words. Skipped {skipped} duplicates.")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
