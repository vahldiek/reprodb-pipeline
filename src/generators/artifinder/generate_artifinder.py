#!/usr/bin/env python3
"""Generate ArtiFinder integration outputs.

Downloads the ArtiFinder-Data set, matches each discovered artifact link to an
existing artifact-evaluation (AE) paper by **title + author list**, and:

  - **back-patches** ``assets/data/artifacts.json`` to add an ``artifinder_urls``
    list to every AE artifact that ArtiFinder found a link for.  These links
    carry no badges and never affect any score; the only place they may be
    reused is repository statistics (GitHub stars/forks), per project policy.
  - writes small Jekyll aggregates for the ArtiFinder discovery page:
    ``_data/artifinder_summary.yml``, ``_data/artifinder_by_year.yml``,
    ``_data/artifinder_by_conference.yml``.

The raw discovered links live in the upstream ArtiFinder-Data repository; we do
not republish them here.

Usage::

    python -m src.generators.artifinder.generate_artifinder --data_dir ../reprodb.github.io/src
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from src.models.artifacts.artifacts import Artifact
from src.scrapers.artifinder import DEFAULT_MIN_YEAR, load_artifinder
from src.scrapers.repo_utils import _normalise_github_repo_url
from src.utils.io.io import load_json, resolve_data_path, save_validated_json, save_yaml
from src.utils.normalization.conference import normalize_name, normalize_title

logger = logging.getLogger(__name__)


def _author_key_set(authors: list[str]) -> set[str]:
    """Return the set of aggressively-normalised author names."""
    return {normalize_name(a) for a in authors if a and normalize_name(a)}


def _build_paper_authors_index(paper_authors: list[dict]) -> dict[str, set[str]]:
    """Map normalised paper title → set of normalised author names."""
    index: dict[str, set[str]] = {}
    for pa in paper_authors:
        title = pa.get("title", "")
        if not title:
            continue
        index[normalize_title(title)] = _author_key_set(pa.get("authors", []))
    return index


def match_entries(
    entries: list[dict],
    artifacts: list[dict],
    authors_by_title: dict[str, set[str]],
) -> list[dict]:
    """Match ArtiFinder entries to AE artifacts by title + author overlap.

    Mutates matched ``artifacts`` in place to append ``artifinder_urls`` and
    returns the per-entry match records (used only to build the aggregates).

    A match requires the same conference, the same year, an identical
    normalised title, and — when author lists are known on both sides — at
    least one shared normalised author name (guards against title collisions).
    """
    # Index artifacts by (conference, year, normalized_title) → artifact dict.
    artifact_index: dict[tuple[str, int, str], dict] = {}
    for art_row in artifacts:
        key = (
            str(art_row.get("conference", "")).upper(),
            int(art_row.get("year", 0)),
            normalize_title(art_row.get("title", "")),
        )
        # Keep the first artifact for a given key (there should only be one).
        artifact_index.setdefault(key, art_row)

    records: list[dict] = []

    for entry in entries:
        conf = str(entry["conference"]).upper()
        year = int(entry["year"])
        nt = normalize_title(entry["title"])
        url = entry["discovered_artifact"]

        art = artifact_index.get((conf, year, nt))
        matched = False
        if art is not None:
            entry_authors = _author_key_set(entry.get("authors", []))
            art_authors = authors_by_title.get(nt, set())
            # Reject the match only when both sides list authors but none overlap.
            if entry_authors and art_authors and not (entry_authors & art_authors):
                art = None
            else:
                matched = True

        records.append(
            {
                "conference": entry["conference"],
                "year": year,
                "artifact_url": url,
                "matched_ae": matched,
            }
        )

        if matched and art is not None:
            existing = set(art.get("artifact_urls", [])) | set(art.get("artifinder_urls", []))
            # Compare on a scheme-insensitive / trailing-slash-insensitive basis.
            norm_existing = {u.rstrip("/") for u in existing}
            if url.rstrip("/") not in norm_existing:
                art.setdefault("artifinder_urls", []).append(url)

    return records


def _build_stats(counts: list[dict], records: list[dict]) -> tuple[dict, list[dict], list[dict]]:
    """Build summary, per-year, and per-conference aggregates for the website."""
    matched_by_cy: dict[tuple[str, int], int] = defaultdict(int)
    github_by_cy: dict[tuple[str, int], int] = defaultdict(int)
    for r in records:
        cy = (r["conference"], r["year"])
        if r["matched_ae"]:
            matched_by_cy[cy] += 1
        if _normalise_github_repo_url(r["artifact_url"]):
            github_by_cy[cy] += 1

    by_year_acc: dict[int, dict] = defaultdict(
        lambda: {"total_papers": 0, "discovered": 0, "matched_ae": 0, "github": 0}
    )
    by_conf_acc: dict[str, dict] = {}
    for c in counts:
        conf, year = c["conference"], c["year"]
        cy = (conf, year)
        y = by_year_acc[year]
        y["total_papers"] += c["total_papers"]
        y["discovered"] += c["discovered"]
        y["matched_ae"] += matched_by_cy.get(cy, 0)
        y["github"] += github_by_cy.get(cy, 0)

        cf = by_conf_acc.setdefault(
            conf,
            {"name": conf, "category": c["category"], "total_papers": 0, "discovered": 0, "matched_ae": 0, "years": []},
        )
        cf["total_papers"] += c["total_papers"]
        cf["discovered"] += c["discovered"]
        cf["matched_ae"] += matched_by_cy.get(cy, 0)
        cf["years"].append(
            {
                "year": year,
                "total_papers": c["total_papers"],
                "discovered": c["discovered"],
                "matched_ae": matched_by_cy.get(cy, 0),
            }
        )

    by_year = [
        {"year": yr, **{k: v[k] for k in ("total_papers", "discovered", "matched_ae", "github")}}
        for yr, v in sorted(by_year_acc.items())
    ]
    by_conf = sorted(by_conf_acc.values(), key=lambda x: x["name"])
    for cf in by_conf:
        cf["years"].sort(key=lambda x: x["year"])

    total_papers = sum(c["total_papers"] for c in counts)
    total_discovered = len(records)
    total_matched = sum(1 for r in records if r["matched_ae"])
    total_github = sum(1 for r in records if _normalise_github_repo_url(r["artifact_url"]))
    years = [c["year"] for c in counts]
    summary = {
        "total_papers": total_papers,
        "total_discovered": total_discovered,
        "total_matched_ae": total_matched,
        "total_unmatched": total_discovered - total_matched,
        "github_count": total_github,
        "discovery_pct": round(100 * total_discovered / total_papers, 1) if total_papers else 0.0,
        "conferences": sorted(by_conf_acc.keys()),
        "year_range": f"{min(years)}\u2013{max(years)}" if years else "",
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }
    return summary, by_year, by_conf


def generate_artifinder(
    data_dir: str,
    min_year: int | None = DEFAULT_MIN_YEAR,
    conf_regex: str | None = None,
    local_dir: str | None = None,
) -> dict:
    """Run the ArtiFinder integration and write all output files."""
    root = Path(data_dir)
    assets_data = root / "assets" / "data"
    jekyll_data = root / "_data"

    data = load_artifinder(conf_regex=conf_regex, min_year=min_year, local_dir=local_dir)
    if not data.entries:
        logger.warning("ArtiFinder: no entries loaded; writing empty outputs")

    artifacts = load_json(assets_data / "artifacts.json", default=[]) or []

    pa_path = resolve_data_path(root, "paper_authors_map.json")
    paper_authors = load_json(pa_path, default=[]) or [] if pa_path.exists() else []
    authors_by_title = _build_paper_authors_index(paper_authors)

    records = match_entries(data.entries, artifacts, authors_by_title)

    # Back-patch artifacts.json (only artifinder_urls were possibly added). This
    # is the single place ArtiFinder links are persisted; the raw links stay in
    # the upstream ArtiFinder-Data repo. The repo_stats stage reads the GitHub
    # links directly from here.
    save_validated_json(assets_data / "artifacts.json", artifacts, Artifact)

    # Website statistics (Jekyll _data) for the ArtiFinder discovery page.
    summary, by_year, by_conf = _build_stats(data.counts, records)
    save_yaml(jekyll_data / "artifinder_summary.yml", summary)
    save_yaml(jekyll_data / "artifinder_by_year.yml", by_year)
    save_yaml(jekyll_data / "artifinder_by_conference.yml", by_conf)

    logger.info(
        "ArtiFinder: %d discovered links (%d matched to AE, %d GitHub), min_year=%s",
        summary["total_discovered"],
        summary["total_matched_ae"],
        summary["github_count"],
        min_year,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate ArtiFinder integration outputs")
    parser.add_argument("--data_dir", type=str, required=True, help="Website output directory")
    parser.add_argument(
        "--min_year",
        type=int,
        default=DEFAULT_MIN_YEAR,
        help=f"Earliest conference edition year to include (default {DEFAULT_MIN_YEAR}).",
    )
    parser.add_argument("--conf_regex", type=str, default=None, help="Optional conference-year filter regex")
    parser.add_argument(
        "--local_dir",
        type=str,
        default=None,
        help="Path to a local ArtiFinder-Data checkout (skips network access). "
        "Defaults to the REPRODB_ARTIFINDER_DIR environment variable.",
    )
    args = parser.parse_args()
    generate_artifinder(args.data_dir, min_year=args.min_year, conf_regex=args.conf_regex, local_dir=args.local_dir)


if __name__ == "__main__":
    from src.utils.io.logging_config import setup_logging

    setup_logging()
    main()
