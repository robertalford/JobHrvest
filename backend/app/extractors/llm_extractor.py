"""
LLM-based job extraction — Stage 4c.

Uses Ollama (local) with the instructor library to extract structured job data
directly into Pydantic models. This is the most flexible extractor — handles
arbitrary page structures that ATS-specific and structural methods miss.
"""

import logging
from typing import Optional, Literal
from datetime import date

import httpx
from pydantic import BaseModel, Field

from app.core.config import settings

logger = logging.getLogger(__name__)


class ExtractedJob(BaseModel):
    """Pydantic model for structured LLM job extraction."""
    title: str = Field(description="The exact job title as shown on the page")
    description: str = Field(description="Full job description text")
    location_raw: Optional[str] = Field(None, description="Raw location string (e.g. 'Sydney, NSW', 'Remote - Australia')")
    is_remote: Optional[bool] = Field(None, description="Is this a remote position?")
    remote_type: Optional[Literal["fully_remote", "hybrid", "onsite", "flexible"]] = Field(None)
    employment_type: Optional[Literal["full_time", "part_time", "contract", "internship", "temporary", "volunteer"]] = Field(None)
    seniority_level: Optional[Literal["entry", "mid", "senior", "lead", "director", "executive"]] = Field(None)
    department: Optional[str] = Field(None, description="Department or team (e.g. Engineering, Marketing)")
    team: Optional[str] = Field(None, description="Specific team within department")
    salary_raw: Optional[str] = Field(None, description="Raw salary string exactly as shown (e.g. '$80,000 - $120,000 + super')")
    requirements: Optional[str] = Field(None, description="Required qualifications and skills")
    benefits: Optional[str] = Field(None, description="Benefits and perks mentioned")
    date_posted: Optional[str] = Field(None, description="Date posted in YYYY-MM-DD format if visible")
    date_expires: Optional[str] = Field(None, description="Application closing date in YYYY-MM-DD format if visible")
    skills_mentioned: list[str] = Field(default_factory=list, description="Technical skills and tools mentioned")
    qualifications: list[str] = Field(default_factory=list, description="Required qualifications/certifications")
    application_url: Optional[str] = Field(None, description="Direct link to apply")


class LLMJobExtractor:
    """
    Structured extraction from arbitrary job pages using Ollama + instructor.

    Falls back to a direct Ollama API call if instructor isn't available.
    """

    EXTRACTION_PROMPT = """You are extracting structured data from a job posting page.

Page URL: {url}
Page Content:
{content}

Extract all available job information. Be precise — only include information actually present on the page.
For salary: preserve the exact string shown (e.g. "$80,000 - $120,000 + super", "$45/hr").
For location: preserve the exact string shown.
For dates: convert to YYYY-MM-DD format.
If a field is not mentioned, leave it as null.
"""

    def __init__(self):
        self.ollama_url = settings.OLLAMA_BASE_URL
        self.model = settings.OLLAMA_MODEL

    async def extract(self, url: str, content: str) -> Optional[dict]:
        """
        Extract structured job data from page content.
        Returns a dict matching our job schema, or None on failure.
        """
        # Truncate content to stay within context window
        truncated = content[:6000] if len(content) > 6000 else content

        # Try instructor first (Pydantic-validated output)
        result = await self._extract_with_instructor(url, truncated)
        if result:
            return self._to_dict(result, method="llm_instructor")

        # Fallback: raw Ollama API with JSON parsing
        result = await self._extract_raw_ollama(url, truncated)
        if result:
            result["extraction_method"] = "llm_raw"
            return result

        return None

    async def _extract_with_instructor(self, url: str, content: str) -> Optional[ExtractedJob]:
        """Use instructor + openai client → Ollama for Pydantic-validated extraction."""
        try:
            import instructor
            from openai import AsyncOpenAI

            client = instructor.from_openai(
                AsyncOpenAI(
                    base_url=f"{self.ollama_url}/v1",
                    api_key="ollama",  # Ollama doesn't need a real key
                ),
                mode=instructor.Mode.JSON,
            )

            result = await client.chat.completions.create(
                model=self.model,
                response_model=ExtractedJob,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precise data extraction assistant. Extract job posting data into the provided schema. Only include information explicitly present in the content.",
                    },
                    {
                        "role": "user",
                        "content": self.EXTRACTION_PROMPT.format(url=url, content=content),
                    },
                ],
                max_retries=2,
            )
            return result
        except Exception as e:
            logger.warning(f"instructor extraction failed for {url}: {e}")
            return None

    async def _extract_raw_ollama(self, url: str, content: str) -> Optional[dict]:
        """Direct Ollama API call with JSON mode as fallback."""
        prompt = self.EXTRACTION_PROMPT.format(url=url, content=content)
        prompt += "\n\nRespond with valid JSON only. Use null for missing fields."

        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "format": "json",
                        "stream": False,
                        "options": {"temperature": 0.1},
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                import json
                return json.loads(data.get("response", "{}"))
        except Exception as e:
            logger.error(f"Raw Ollama extraction failed for {url}: {e}")
            return None

    def _to_dict(self, job: ExtractedJob, method: str) -> dict:
        """Convert ExtractedJob Pydantic model to our internal dict format."""
        return {
            "title": job.title,
            "description": job.description,
            "location_raw": job.location_raw,
            "is_remote": job.is_remote,
            "remote_type": job.remote_type,
            "employment_type": job.employment_type,
            "seniority_level": job.seniority_level,
            "department": job.department,
            "team": job.team,
            "salary_raw": job.salary_raw,
            "requirements": job.requirements,
            "benefits": job.benefits,
            "date_posted": job.date_posted,
            "date_expires": job.date_expires,
            "application_url": job.application_url,
            "skills_mentioned": job.skills_mentioned,
            "qualifications": job.qualifications,
            "extraction_method": method,
            "extraction_confidence": 0.80,
        }


class LLMPageClassifier:
    """
    LLM-based career page classifier — Stage 2b.

    Classifies a page as: CAREERS_LISTING, CAREERS_LANDING, SINGLE_JOB,
    CAREERS_RELATED, or NOT_CAREERS.
    """

    CLASSIFICATION_PROMPT = """You are a classifier that determines if a web page is a careers/jobs page.

Page URL: {url}
Page Title: {title}
Page Content (truncated):
{content}

Classify this page into exactly one category:
- CAREERS_LISTING: A page that lists multiple job openings/positions
- CAREERS_LANDING: A careers landing page that links to job listings but doesn't list jobs itself
- SINGLE_JOB: A page for a single specific job posting
- CAREERS_RELATED: Related to careers (about the team, culture, benefits) but no job listings
- NOT_CAREERS: Not related to careers or jobs

Respond with JSON only:
{"classification": "...", "confidence": 0.0, "reasoning": "one sentence"}"""

    def __init__(self):
        self.ollama_url = settings.OLLAMA_BASE_URL
        self.model = settings.OLLAMA_MODEL

    async def classify(self, url: str, title: str, content: str) -> dict:
        """
        Classify a page. Returns {"classification": str, "confidence": float, "reasoning": str}
        """
        truncated = content[:2000]
        prompt = self.CLASSIFICATION_PROMPT.format(url=url, title=title, content=truncated)

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "format": "json",
                        "stream": False,
                        "options": {"temperature": 0.0},
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                import json
                result = json.loads(data.get("response", "{}"))
                return {
                    "classification": result.get("classification", "NOT_CAREERS"),
                    "confidence": float(result.get("confidence", 0.5)),
                    "reasoning": result.get("reasoning", ""),
                }
        except Exception as e:
            logger.warning(f"LLM page classification failed for {url}: {e}")
            return {"classification": "NOT_CAREERS", "confidence": 0.0, "reasoning": f"LLM error: {e}"}

    async def classify_and_store(self, db, career_page, html: str) -> dict:
        """Classify a career page and update its record in the database."""
        from bs4 import BeautifulSoup
        from markdownify import markdownify

        soup = BeautifulSoup(html, "lxml")
        title = soup.find("title")
        title_text = title.get_text(strip=True) if title else ""
        markdown = markdownify(html, strip=["script", "style"])

        result = await self.classify(career_page.url, title_text, markdown)

        # Update page type in DB
        type_map = {
            "CAREERS_LISTING": "listing_page",
            "CAREERS_LANDING": "listing_page",
            "SINGLE_JOB": "single_job_page",
            "CAREERS_RELATED": "listing_page",
            "NOT_CAREERS": None,
        }
        career_page.page_type = type_map.get(result["classification"], "listing_page")
        career_page.discovery_confidence = max(
            career_page.discovery_confidence or 0,
            result["confidence"] if result["classification"] in ("CAREERS_LISTING", "CAREERS_LANDING") else 0,
        )
        await db.commit()

        return result
