# Data Schemas

All pipeline output formats are defined as [Pydantic](https://docs.pydantic.dev/) models
in `src/models/`. JSON Schema files are auto-generated from these models.

## Models

::: src.models.artifacts.artifacts
    options:
      show_root_heading: true
      members: [Artifact]

::: src.models.aggregates.summary
    options:
      show_root_heading: true
      members: [Summary]

::: src.models.aggregates.artifacts_by_conference
    options:
      show_root_heading: true
      members: [ConferenceEntry, YearBreakdown]

::: src.models.aggregates.artifacts_by_year
    options:
      show_root_heading: true
      members: [YearCount]

::: src.models.authors.author_index
    options:
      show_root_heading: true
      members: [AuthorIndexEntry, AffiliationHistoryEntry, ExternalIds]

::: src.models.authors.author_stats
    options:
      show_root_heading: true
      members: [AuthorStats, ArtifactPaper, PlainPaper]

::: src.models.artifacts.paper_index
    options:
      show_root_heading: true
      members: [Paper]

::: src.models.authors.combined_rankings
    options:
      show_root_heading: true
      members: [AuthorRanking]

::: src.models.institutions.institution_rankings
    options:
      show_root_heading: true
      members: [InstitutionRanking, TopAuthor]

::: src.models.aggregates.repo_stats
    options:
      show_root_heading: true
      members: [RepoStatsEntry, RepoStatsSummary, OverallStats, ConferenceRepoStats, YearRepoStats]

::: src.models.artifacts.search_data
    options:
      show_root_heading: true
      members: [SearchEntry]
