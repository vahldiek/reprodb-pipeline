"""Tests for the ArtiFinder integration generator (``generate_artifinder``)."""

from __future__ import annotations

import json

import pytest
import yaml

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


def _entry(conf, year, title, authors, url):
    return {
        "conference": conf,
        "category": "security",
        "year": year,
        "title": title,
        "authors": authors,
        "page_link": None,
        "discovered_artifact": url,
    }


class TestMatchEntries:
    def test_matches_and_backpatches_artifact(self):
        artifacts = [_artifact("NDSS", 2023, "A Great Paper.", ["available"], [], paper_id=7)]
        # ArtiFinder title lacks the trailing period; normalize_title handles it.
        entries = [_entry("NDSS", 2023, "A Great Paper", ["Jane Doe"], "https://github.com/org/repo")]
        authors_by_title = {g.normalize_title("A Great Paper."): {g.normalize_name("Jane Doe")}}
        records = g.match_entries(entries, artifacts, authors_by_title)
        assert records[0]["matched_ae"] is True
        assert artifacts[0]["artifinder_urls"] == ["https://github.com/org/repo"]

    def test_no_author_overlap_rejects_match(self):
        artifacts = [_artifact("NDSS", 2023, "Same Title.", ["available"], [], paper_id=1)]
        entries = [_entry("NDSS", 2023, "Same Title.", ["Totally Different"], "https://github.com/x/y")]
        authors_by_title = {g.normalize_title("Same Title."): {g.normalize_name("Original Author")}}
        records = g.match_entries(entries, artifacts, authors_by_title)
        assert records[0]["matched_ae"] is False
        assert artifacts[0].get("artifinder_urls", []) == []

    def test_year_mismatch_not_matched(self):
        artifacts = [_artifact("NDSS", 2022, "Paper.", ["available"], [], paper_id=1)]
        entries = [_entry("NDSS", 2023, "Paper.", ["Jane Doe"], "https://github.com/x/y")]
        records = g.match_entries(entries, artifacts, {})
        assert records[0]["matched_ae"] is False

    def test_does_not_duplicate_existing_url(self):
        artifacts = [_artifact("NDSS", 2023, "Paper.", ["available"], ["https://github.com/x/y"], paper_id=1)]
        # trailing-slash variant of an already-present URL
        entries = [_entry("NDSS", 2023, "Paper.", [], "https://github.com/x/y/")]
        records = g.match_entries(entries, artifacts, {})
        assert records[0]["matched_ae"] is True
        assert artifacts[0].get("artifinder_urls", []) == []  # already present, not duplicated

    def test_badges_never_changed(self):
        artifacts = [_artifact("NDSS", 2023, "Paper.", ["available", "functional"], [], paper_id=1)]
        entries = [_entry("NDSS", 2023, "Paper.", [], "https://github.com/x/y")]
        g.match_entries(entries, artifacts, {})
        assert artifacts[0]["badges"] == ["available", "functional"]


class TestAuthorIndex:
    def test_author_key_normalises_accents_and_case(self):
        assert g._author_key("Manuel Vögele") == "manuel vogele"
        assert g._author_key("Anjo Vahldiek-Oberwagner") == "anjo vahldiek oberwagner"

    def test_build_author_index_only_unmatched(self):
        entries = [
            _entry("NDSS", 2023, "Matched.", ["Jane Doe"], "https://github.com/a/b"),
            _entry("CCS", 2023, "Unmatched.", ["John Roe", "Jane Doe"], "https://github.com/c/d"),
        ]
        records = [{"matched_ae": True}, {"matched_ae": False}]
        idx = g._build_author_index(entries, records)
        # Only the unmatched paper's authors are indexed.
        assert set(idx.keys()) == {"john roe", "jane doe"}
        assert idx["jane doe"][0]["title"] == "Unmatched."
        assert idx["john roe"][0]["url"] == "https://github.com/c/d"


class TestGenerateArtifinderEndToEnd:
    @pytest.fixture
    def _patch_loader(self, monkeypatch):
        entries = [
            _entry(
                "USENIXSEC",
                2023,
                "Framing Frames.",
                ["Domien Schepers", "Mathy Vanhoef"],
                "https://github.com/domienschepers/wifi-framing",
            ),
            # CCS is not tracked for AE → stays unmatched.
            _entry("CCS", 2023, "Untracked Venue Paper.", ["Someone Else"], "https://github.com/foo/bar"),
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
        arts = [_artifact("USENIXSEC", 2023, "Framing Frames.", ["available", "functional"], [], paper_id=42)]
        (tmp_website / "assets" / "data" / "artifacts.json").write_text(json.dumps(arts))
        (tmp_website / "_build" / "paper_authors_map.json").write_text(
            json.dumps([{"title": "Framing Frames.", "authors": ["Domien Schepers", "Mathy Vanhoef"], "doi_url": ""}])
        )

        summary = g.generate_artifinder(str(tmp_website), min_year=2017)

        # The ONLY persisted artifact of the discovered links: artifacts.json
        # gains artifinder_urls; badges are untouched.
        back = json.loads((tmp_website / "assets" / "data" / "artifacts.json").read_text())
        assert back[0]["badges"] == ["available", "functional"]
        assert back[0]["artifinder_urls"] == ["https://github.com/domienschepers/wifi-framing"]

        # No redundant JSON dump of the source data is written.
        assert not (tmp_website / "assets" / "data" / "artifinder.json").exists()
        assert not (tmp_website / "_build" / "artifinder_matched_urls.json").exists()

        # Jekyll aggregates for the discovery page.
        by_year = yaml.safe_load((tmp_website / "_data" / "artifinder_by_year.yml").read_text())
        assert by_year == [{"year": 2023, "total_papers": 7, "discovered": 2, "matched_ae": 1, "github": 2}]
        by_conf = yaml.safe_load((tmp_website / "_data" / "artifinder_by_conference.yml").read_text())
        assert {c["name"] for c in by_conf} == {"USENIXSEC", "CCS"}

        assert summary["total_discovered"] == 2
        assert summary["total_matched_ae"] == 1
        assert summary["total_papers"] == 7
        assert set(summary["conferences"]) == {"USENIXSEC", "CCS"}

        # The unmatched (non-AE) CCS paper becomes a marked search row.
        se = json.loads((tmp_website / "_build" / "artifinder_search_entries.json").read_text())
        assert len(se) == 1
        assert se[0]["conference"] == "CCS"
        assert se[0]["badges"] == []
        assert se[0]["artifact_urls"] == []
        assert se[0]["artifinder_urls"] == ["https://github.com/foo/bar"]
        assert se[0]["authors"] == ["Someone Else"]

        # Author-indexed non-AE discoveries for profile pages.
        aa = json.loads((tmp_website / "assets" / "data" / "artifinder_authors.json").read_text())
        assert "someone else" in aa
        assert aa["someone else"][0]["title"] == "Untracked Venue Paper."
        assert aa["someone else"][0]["url"] == "https://github.com/foo/bar"

    def test_missing_artifacts_file_is_safe(self, tmp_website, _patch_loader):
        summary = g.generate_artifinder(str(tmp_website), min_year=2017)
        assert summary["total_matched_ae"] == 0
        assert (tmp_website / "_data" / "artifinder_by_year.yml").exists()
        assert not (tmp_website / "assets" / "data" / "artifinder.json").exists()
