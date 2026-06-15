"""Tests for the IEEE S&P 2026 committee scraper."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.scrapers.scrape_committee_web import scrape_sp_committee
from src.utils.io.cache import _MISSING


@pytest.fixture(autouse=True)
def _no_disk_cache():
    """Prevent disk cache from interfering between tests."""
    with (
        patch("src.utils.io.cache.read_cache", return_value=_MISSING),
        patch("src.utils.io.cache.write_cache"),
    ):
        yield


def _mock_session(member_html: str, chair_html: str):
    """Build a mock session that returns different responses per URL."""
    session = MagicMock()

    def fake_get(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        if url.endswith("cfartifacts.html"):
            resp.text = member_html
            resp.json.return_value = json.loads("{}")
        elif url.endswith("index.html"):
            resp.text = chair_html
            resp.json.return_value = json.loads("{}")
        else:
            resp.text = ""
            resp.json.return_value = json.loads("{}")
        return resp

    session.get = fake_get
    return session


def test_sp_2026_members_and_chairs():
    member_html = """
    <h2>Artifact Evaluation Committee</h2>
    <h4>Cycle 1</h4>
    <table>
      <tr><td>Alice A</td><td>North Carolina State University</td></tr>
      <tr><td>Bob B</td><td>University of California Irvine</td></tr>
    </table>
    <h4>Cycle 2</h4>
    <table>
      <tr><td>Alice A</td><td>North Carolina State University, USA</td></tr>
      <tr><td>Carol C</td><td>University of Wisconsin-Madison</td></tr>
    </table>
    """
    chair_html = """
    <table>
      <tr><td>General Chair</td><td>Alina Oprea</td><td>Northeastern University</td></tr>
      <tr><td>Artifact Evaluation Chairs</td><td>Jelena Mirkovic</td><td>University of Southern California</td></tr>
      <tr><td></td><td>Manuel Egele</td><td>Boston University</td></tr>
      <tr><td>Web Chair</td><td>Andreas Brüggemann</td><td>TU Darmstadt</td></tr>
    </table>
    """
    session = _mock_session(member_html, chair_html)

    result = scrape_sp_committee(2026, session=session)

    assert result is not None
    names = {entry["name"] for entry in result}
    assert names == {"Alice A", "Bob B", "Carol C", "Jelena Mirkovic", "Manuel Egele"}

    chairs = [entry for entry in result if entry["role"] == "chair"]
    assert len(chairs) == 2
    assert {entry["name"] for entry in chairs} == {"Jelena Mirkovic", "Manuel Egele"}

    members = [entry for entry in result if entry["role"] == "member"]
    assert len(members) == 3
