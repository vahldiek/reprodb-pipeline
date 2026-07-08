"""Tests for src/generators/generate_search_data."""

import json

import pytest

from src.generators.output.generate_search_data import _title_key, generate_search_data


class TestTitleKey:
    def test_basic(self):
        assert _title_key("My Cool Paper") == "mycoolpaper"

    def test_punctuation_stripped(self):
        assert _title_key("Hello, World! (2023)") == "helloworld2023"

    def test_case_insensitive(self):
        assert _title_key("ABC") == _title_key("abc")

    def test_empty(self):
        assert _title_key("") == ""


class TestGenerateSearchData:
    @pytest.fixture()
    def data_dir(self, tmp_path):
        assets = tmp_path / "assets" / "data"
        assets.mkdir(parents=True)
        return tmp_path

    def _write(self, data_dir, filename, data):
        path = data_dir / "assets" / "data" / filename
        with open(path, "w") as f:
            json.dump(data, f)

    def test_basic_merge(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "My Paper",
                    "conference": "OSDI",
                    "category": "systems",
                    "year": 2023,
                    "badges": ["available"],
                    "artifact_urls": ["https://github.com/user/repo"],
                },
            ],
        )
        self._write(
            data_dir,
            "paper_authors_map.json",
            [{"title": "My Paper", "authors": ["Alice Smith"], "doi_url": "https://doi.org/10.1234/test"}],
        )
        self._write(
            data_dir,
            "authors.json",
            [{"name": "Alice Smith", "display_name": "Alice Smith", "affiliation": "MIT"}],
        )

        result = generate_search_data(str(data_dir))
        assert len(result) == 1
        assert result[0]["authors"] == ["Alice Smith"]
        assert result[0]["affiliations"] == ["MIT"]
        assert result[0]["doi_url"] == "https://doi.org/10.1234/test"

    def test_missing_authors_file(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "Paper",
                    "conference": "SOSP",
                    "category": "systems",
                    "year": 2023,
                    "badges": [],
                    "artifact_urls": [],
                },
            ],
        )
        result = generate_search_data(str(data_dir))
        assert len(result) == 1
        assert result[0]["authors"] == []
        assert result[0]["affiliations"] == []

    def test_disambiguation_suffix_stripped(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "Paper",
                    "conference": "OSDI",
                    "category": "systems",
                    "year": 2023,
                    "badges": [],
                    "artifact_urls": [],
                },
            ],
        )
        self._write(
            data_dir,
            "paper_authors_map.json",
            [{"title": "Paper", "authors": ["Haibo Chen 0001"]}],
        )
        result = generate_search_data(str(data_dir))
        assert result[0]["authors"] == ["Haibo Chen"]

    def test_artifinder_urls_passthrough(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "Paper",
                    "conference": "USENIXSEC",
                    "category": "security",
                    "year": 2023,
                    "badges": ["available"],
                    "artifact_urls": ["https://github.com/a/b"],
                    "artifinder_urls": ["https://github.com/found/by-artifinder"],
                },
            ],
        )
        result = generate_search_data(str(data_dir))
        assert result[0]["artifinder_urls"] == ["https://github.com/found/by-artifinder"]

    def test_no_artifinder_urls_key_when_empty(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "Paper",
                    "conference": "USENIXSEC",
                    "category": "security",
                    "year": 2023,
                    "badges": ["available"],
                    "artifact_urls": [],
                    "artifinder_urls": [],
                },
            ],
        )
        result = generate_search_data(str(data_dir))
        assert "artifinder_urls" not in result[0]

    def test_sorted_by_year_desc(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "Old",
                    "conference": "OSDI",
                    "category": "systems",
                    "year": 2020,
                    "badges": [],
                    "artifact_urls": [],
                },
                {
                    "title": "New",
                    "conference": "OSDI",
                    "category": "systems",
                    "year": 2024,
                    "badges": [],
                    "artifact_urls": [],
                },
            ],
        )
        result = generate_search_data(str(data_dir))
        assert result[0]["year"] == 2024
        assert result[1]["year"] == 2020

    def test_output_file_written(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "Paper",
                    "conference": "OSDI",
                    "category": "systems",
                    "year": 2023,
                    "badges": ["available"],
                    "artifact_urls": [],
                },
            ],
        )
        generate_search_data(str(data_dir))
        out = data_dir / "assets" / "data" / "search_data.json"
        assert out.exists()
        with open(out) as f:
            data = json.load(f)
        assert len(data) == 1

    def test_appends_artifinder_only_entries(self, data_dir):
        self._write(
            data_dir,
            "artifacts.json",
            [
                {
                    "title": "AE Paper",
                    "conference": "USENIXSEC",
                    "category": "security",
                    "year": 2023,
                    "badges": ["available"],
                    "artifact_urls": ["https://github.com/a/b"],
                },
            ],
        )
        build = data_dir / "_build"
        build.mkdir(parents=True, exist_ok=True)
        (build / "artifinder_search_entries.json").write_text(
            json.dumps(
                [
                    {
                        "title": "Discovered Only",
                        "conference": "CCS",
                        "category": "security",
                        "year": 2022,
                        "badges": [],
                        "artifact_urls": [],
                        "artifinder_urls": ["https://github.com/x/y"],
                        "doi_url": "",
                        "authors": ["Jane Doe"],
                        "affiliations": [],
                        "source": "artifinder",
                    }
                ]
            )
        )
        result = generate_search_data(str(data_dir))
        titles = {e["title"] for e in result}
        assert titles == {"AE Paper", "Discovered Only"}
        disc = next(e for e in result if e["title"] == "Discovered Only")
        assert disc["artifinder_urls"] == ["https://github.com/x/y"]
        assert disc["badges"] == []
        assert disc["source"] == "artifinder"
        ae = next(e for e in result if e["title"] == "AE Paper")
        assert ae["source"] == "ae"
