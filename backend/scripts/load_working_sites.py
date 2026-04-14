"""Load proven working sites from validation output into fixed_test_sites table."""
import asyncio
import json

async def main():
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text

    try:
        with open("/storage/working_test_sites.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("No working sites file yet. Run find_working_sites.py first.")
        return

    passed = data.get("passed_sites", [])
    if not passed:
        print("No passed sites found.")
        return

    print(f"Loading {len(passed)} proven working sites into fixed_test_sites...")

    async with AsyncSessionLocal() as db:
        await db.execute(text("DELETE FROM fixed_test_sites"))

        loaded = 0
        for site in passed:
            url = site["url"]
            company = site["company"]
            r = await db.execute(text("""
                SELECT sw.selectors FROM site_wrapper_test_data sw
                JOIN crawler_test_data ct ON ct.external_id = sw.crawler_id
                JOIN site_url_test_data su ON su.site_id = ct.job_site_id
                WHERE su.url = :url LIMIT 1
            """), {"url": url})
            row = r.first()
            if row:
                sels = row[0]
                sels_str = json.dumps(sels) if isinstance(sels, dict) else sels
                await db.execute(text(
                    "INSERT INTO fixed_test_sites (url, company_name, known_selectors) VALUES (:u, :c, cast(:s as jsonb))"
                ), {"u": url, "c": company, "s": sels_str})
                loaded += 1

        await db.commit()
        r = await db.execute(text("SELECT COUNT(*) FROM fixed_test_sites"))
        print(f"Loaded {r.scalar()} sites into fixed_test_sites (of {len(passed)} passed)")

asyncio.run(main())
