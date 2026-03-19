"""
Tag extractor — Stage 4 (skills, technologies, qualifications).

Extracts structured tags from job descriptions using pattern matching
and a curated keyword library. ML-based extraction is added in Phase 7.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ExtractedTags:
    skills: list[str] = field(default_factory=list)
    technologies: list[str] = field(default_factory=list)
    qualifications: list[str] = field(default_factory=list)
    industries: list[str] = field(default_factory=list)


# Technology / tool keywords
TECH_KEYWORDS = {
    # Languages
    "python", "javascript", "typescript", "java", "go", "golang", "rust", "c#", "c++",
    "ruby", "php", "swift", "kotlin", "scala", "r", "matlab",
    # Frameworks
    "react", "angular", "vue", "next.js", "fastapi", "django", "flask", "spring",
    "rails", "laravel", "express", ".net", "node.js", "nestjs",
    # Cloud / Infra
    "aws", "azure", "gcp", "google cloud", "kubernetes", "docker", "terraform",
    "ansible", "jenkins", "github actions", "ci/cd", "devops",
    # Data
    "sql", "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "kafka",
    "spark", "airflow", "dbt", "snowflake", "bigquery", "databricks",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch", "llm",
    # Tools
    "git", "jira", "confluence", "figma", "tableau", "power bi", "salesforce",
    "hubspot", "shopify", "wordpress",
}

# Skill keywords
SKILL_KEYWORDS = {
    "leadership", "communication", "collaboration", "problem solving", "analytical",
    "project management", "agile", "scrum", "stakeholder management", "negotiation",
    "time management", "attention to detail", "critical thinking", "strategic thinking",
    "customer service", "presentation", "mentoring", "coaching", "budgeting",
    "forecasting", "data analysis", "machine learning", "artificial intelligence",
}

# Qualification patterns
QUAL_PATTERNS = [
    re.compile(r"(bachelor|degree|masters|phd|doctorate|diploma|certificate|cert\.?)\s+(?:of\s+)?(?:in\s+)?(\w+[\w\s]*)", re.I),
    re.compile(r"(b\.?sc|b\.?eng|b\.?com|m\.?sc|m\.?ba|b\.?a\.?|b\.?tech)\b", re.I),
    re.compile(r"(cpa|ca|cia|cfa|pmp|prince2|cissp|aws certified|azure certified)\b", re.I),
    re.compile(r"(\d+\+?\s*years?)\s+(?:of\s+)?experience", re.I),
]

# Industry keywords
INDUSTRY_KEYWORDS = {
    "fintech": ["fintech", "financial technology", "payments", "banking technology"],
    "healthcare": ["healthcare", "medical", "clinical", "hospital", "aged care", "disability"],
    "mining": ["mining", "resources", "oil", "gas", "petroleum", "exploration"],
    "education": ["education", "university", "school", "teaching", "learning"],
    "government": ["government", "public sector", "federal", "state government", "council"],
    "retail": ["retail", "ecommerce", "e-commerce", "consumer"],
    "construction": ["construction", "building", "civil", "infrastructure"],
    "agriculture": ["agriculture", "farming", "agtech", "food production"],
}


class TagExtractor:
    """Extract structured tags from job text."""

    def extract(self, title: str, description: str, requirements: str = "") -> ExtractedTags:
        text = f"{title} {description} {requirements}".lower()
        result = ExtractedTags()

        # Technologies
        for tech in TECH_KEYWORDS:
            if re.search(r"\b" + re.escape(tech) + r"\b", text, re.I):
                result.technologies.append(tech)

        # Skills
        for skill in SKILL_KEYWORDS:
            if re.search(r"\b" + re.escape(skill) + r"\b", text, re.I):
                result.skills.append(skill)

        # Qualifications
        full_text = f"{description} {requirements}"
        for pattern in QUAL_PATTERNS:
            for match in pattern.finditer(full_text):
                qual = match.group(0).strip()
                if qual and qual not in result.qualifications:
                    result.qualifications.append(qual)

        # Industry
        for industry, keywords in INDUSTRY_KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    result.industries.append(industry)
                    break

        # Deduplicate
        result.technologies = list(dict.fromkeys(result.technologies))
        result.skills = list(dict.fromkeys(result.skills))
        result.qualifications = list(dict.fromkeys(result.qualifications))[:10]
        result.industries = list(dict.fromkeys(result.industries))

        return result

    def to_tag_dicts(self, tags: ExtractedTags, confidence: float = 0.7) -> list[dict]:
        """Convert to list of dicts for JobTag model insertion."""
        result = []
        for tech in tags.technologies:
            result.append({"tag_type": "technology", "tag_value": tech, "confidence": confidence})
        for skill in tags.skills:
            result.append({"tag_type": "skill", "tag_value": skill, "confidence": confidence})
        for qual in tags.qualifications:
            result.append({"tag_type": "qualification", "tag_value": qual, "confidence": confidence * 0.9})
        for ind in tags.industries:
            result.append({"tag_type": "industry", "tag_value": ind, "confidence": confidence})
        return result


# Module-level singleton
tag_extractor = TagExtractor()
