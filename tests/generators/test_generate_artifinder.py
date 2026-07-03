"""Tests for the ArtiFinder integration generator (``generate_artifinder``)."""

from __future__ import annotations

import json

import pytest

import src.scrapers.artifinder as af
from src.generators.artifinder import generate_artifinder as g


def _artifact(conf, year, title, badges, urls, paper_id=None):
    d = {
        "conference": conf,
        "category": "security",
        "year": year,
        "title": title,
        "badges": badges,
        "artifact_urls": urls,
        "doi": "",
    }
    if paper_id is not None:
        d["paper_id"] = paper_id
    return d


class TestMatchEntries:
    def test_matches_by_title_conf_year_and_author_overlap(self):
        artifacts = [_artifact("NDSS", 2023, "A Great Paper.", ["available"], [], paper_id=7)]
        entries = [
            {
                "conference": "NDSS",
                "category": "security",
                "year": 2023,
                "title": "A Great Paper",  # no trailing period → normalize_title handles it
                "authors": ["Jane Doe"],
                "page_link": None,
                "discovered_artifact": "https://github.com/org/repo",
            }
        ]
        authors_by_title = {g.normalize_title("A Great Paper."): {g.normalize_name("Jane Doe")}}
        records, matched_github = g.match_entries(entries, artifacts, authors_by_title)
        assert records[0]["matched_ae"] is True
        assert records[0]["paper_id"] == 7
        assert artifacts[0]["artifinder_urls"] == ["https://github.com/org/repo"]
        assert matched_github == [
            {"conference": "NDSS", "year": 2023, "title": "A Great Paper", "url": "https://github.com/org/repo"}
        ]

    def test_no_author_overlap_rejects_match(self):
        artifacts = [_artifact("NDSS", 2023, "Same Title.", ["available"], [], paper_id=1)]
        entries = [
            {
                "conference": "NDSS",
                "category": "security",
                "year": 2023,
                "title": "Same Title.",
                "authors": ["Totally Different"],
                "page_link": None,
                "discovered_artifact": "https://github.com/x/y",
            }
        ]
        authors_by_title = {g.normalize_title("Same Title."): {g.normalize_name("Original Author")}}
        records, matched_github = g.match_entries(entries, artifacts, authors_by_title)
        assert records[0]["matched_ae"] is False
        assert records[0]["paper_id"] is None
        assert artifacts[0].get("artifinder_urls", []) == []
        assert matched_github == []

    def test_year_mismatch_not_matched(self):
        artifacts = [_artifact("NDSS", 2022, "Paper.", ["available"], [], paper_id=1)]
        entries = [
            {
                "conference": "NDSS",
                "category": "security",
                "year": 2023,
                "title": "Paper.",
                "authors": ["Jane Doe"],
                "page_link": None,
                "discovered_artifact": "https://github.com/x/y",
            }
        ]
        records, _ = g.match_entries(entries, artifacts, {})
        assert records[0]["matched_ae"] is False

    def test_does_not_duplicate_existing_url(self):
        artifacts = [_artifact("NDSS", 2023, "Paper.", ["available"], ["https://github.com/x/y"], paper_id=1)]
        entries = [
            {
                "conference": "NDSS",
                "category": "security",
                "year": 2023,
                "title": "Paper.",
                "authors": [],
                "page_link": None,
                "discovered_artifact": "https://github.com/x/y/",  # trailing slash variant
            }
        ]
        records, _ = g.match_entries(entries, artifacts, {})
        assert records[0]["matched_ae"] is True
        assert artifacts[0].get("artifinder_urls", []) == []  # already present, not duplicated

    def test_non_github_matched_url_not_in_repo_stats_handoff(self):
        artifacts = [_artifact("NDSS", 2023, "Paper.", ["available"], [], paper_id=1)]
        entries = [
            {
                "conference": "NDSS",
                "category": "security",
                "year": 2023,
                "title": "Paper.",
                "authors": [],
                "page_link": None,
                "discovered_artifact": "https://zenodo.org/record/123",
            }
        ]
        records, matched_github = g.match_entries(entries, artifacts, {})
        assert records[0]["matched_ae"] is True
        assert artifacts[0]["artifinder_urls"] == ["https://zenodo.org/record/123"]
        assert matched_github == []  # non-GitHub → not used for repo stats


class TestGenerateArtifinderEndToEnd:
    @pytest.fixture
    def _patch_loader(self, monkeypatch):
        entries = [
            {
                "conference": "USENIXSEC",
                "category": "security",
                "year": 2023,
                "title": "Framing Frames.",
                "authors": ["Domien Schepers", "Mathy Vanhoef"],
                "page_link": "https://www.usenix.org/x",
                "discovered_artifact": "https://github.com/domienschepers/wifi-framing",
            },
            {
                "conference": "CCS",  # not tracked for AE → stays unmatched
                "category": "security",
                "year": 2023,
                "title": "Untracked Venue Paper.",
                "authors": ["Someone Else"],
                "page_link": None,
                "discovered_artifact": "https://github.com/foo/bar",
            },
        ]
        counts = [
            {"conference": "USENIXSEC", "category": "security", "year": 2023, "total_papers": 2, "discovered": 1},
            {"conference": "CCS", "category": "security", "year": 2023, "total_papers": 5, "discovered": 1},
        ]
        monkeypatch.setattr(
            g,
            "load_artifinder",
            lambda conf_regex=None, min_year=None, local_dir=None: af.ArtiFinderData(entries, counts),
        )

    def test_full_run(self, tmp_website, _patch_loader):
        (tmp_website / "_build").mkdir()
        # AE artifact for the USENIXSEC paper (badges must be preserved).
        arts = [_artifact("USENIXSEC", 2023, "Framing Frames.", ["available", "functional"], [], paper_id=42)]
        (tmp_website / "assets" / "data" / "artifacts.json").write_text(json.dumps(arts))
        (tmp_website / "_build" / "paper_authors_map.json").write_text(
            json.dumps([{"title": "Framing Frames.", "authors": ["Domien Schepers", "Mathy Vanhoef"], "doi_url": ""}])
        )

        summary = g.generate_artifinder(str(tmp_website), min_year=2017)

        # Back-patched artifact keeps its badges and gains the artifinder URL.
        back = json.loads((tmp_website / "assets" / "data" / "artifacts.json").read_text())
        assert back[0]["badges"] == ["available", "functional"]
        assert back[0]["artifinder_urls"] == ["https://github.com/domienschepers/wifi-framing"]

        # artifinder.json contains both entries; one matched, one not.
        af_json = json.loads((tmp_website / "assets" / "data" / "artifinder.json").read_text())
        assert len(af_json) == 2
        matched = [r for r in af_json if r["matched_ae"]]
        assert len(matched) == 1
        assert matched[0]["paper_id"] == 42
        assert matched[0]["conference"] == "USENIXSEC"

        # repo-stats hand-off contains only the AE-matched GitHub link.
        mg = json.loads((tmp_website / "_build" / "artifinder_matched_urls.json").read_text())
        assert mg == [
            {
                "conference": "USENIXSEC",
                "year": 2023,
                "title": "Framing Frames.",
                "url": "https://github.com/domienschepers/wifi-framing",
            }
        ]

        # summary aggregates.
        assert summary["total_discovered"] == 2
        assert summary["total_matched_ae"] == 1
        assert summary["total_papers"] == 7
        assert set(summary["conferences"]) == {"USENIXSEC", "CCS"}

    def test_missing_artifacts_file_is_safe(self, tmp_website, _patch_loader):
        (tmp_website / "_build").mkdir()
        # No artifacts.json → nothing matches, but outputs are still written.
        summary = g.generate_artifinder(str(tmp_website), min_year=2017)
        assert summary["total_matched_ae"] == 0
        assert (tmp_website / "assets" / "data" / "artifinder.json").exists()
        assert (tmp_website / "_data" / "artifinder_by_year.yml").exists()
