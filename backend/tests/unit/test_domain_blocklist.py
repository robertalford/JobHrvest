"""Unit tests for the domain blocklist — critical security test."""

import pytest
from app.crawlers.domain_blocklist import is_blocked, assert_not_blocked


class TestDomainBlocklist:
    # SEEK — all variants must be blocked
    def test_seek_main(self):
        assert is_blocked("https://www.seek.com.au/jobs") is True

    def test_seek_subdomain(self):
        assert is_blocked("https://talent.seek.com.au/") is True

    def test_seek_employer(self):
        assert is_blocked("https://employer.seek.com.au/") is True

    # Jora
    def test_jora_main(self):
        assert is_blocked("https://jora.com/jobs") is True

    def test_jora_au(self):
        assert is_blocked("https://au.jora.com/jobs") is True

    def test_jora_subdomain(self):
        assert is_blocked("https://api.jora.com/v1") is True

    # Jobstreet
    def test_jobstreet_main(self):
        assert is_blocked("https://www.jobstreet.com/jobs") is True

    def test_jobstreet_au(self):
        assert is_blocked("https://jobstreet.com.au/jobs") is True

    def test_jobstreet_my(self):
        assert is_blocked("https://jobstreet.com.my/jobs") is True

    # JobsDB
    def test_jobsdb_main(self):
        assert is_blocked("https://www.jobsdb.com/jobs") is True

    def test_jobsdb_hk(self):
        assert is_blocked("https://hk.jobsdb.com/jobs") is True

    # Permitted sites — must NOT be blocked
    def test_indeed_au_allowed(self):
        assert is_blocked("https://au.indeed.com/jobs") is False

    def test_linkedin_allowed(self):
        assert is_blocked("https://www.linkedin.com/jobs") is False

    def test_company_site_allowed(self):
        assert is_blocked("https://careers.atlassian.com") is False

    def test_canva_allowed(self):
        assert is_blocked("https://www.canva.com/careers") is False

    def test_greenhouse_allowed(self):
        assert is_blocked("https://boards.greenhouse.io/canva") is False

    # assert_not_blocked raises for blocked domains
    def test_assert_raises_for_seek(self):
        with pytest.raises(ValueError, match="blocked"):
            assert_not_blocked("https://www.seek.com.au/jobs")

    def test_assert_passes_for_allowed(self):
        assert_not_blocked("https://au.indeed.com/jobs")  # Should not raise
