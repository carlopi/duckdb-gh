# duckdb-gh

> **Experimental:** this extension is in early development. APIs, behaviour, and URL formats may change without notice. Use with caution.

A [DuckDB](https://duckdb.org) extension that adds a `gh://` filesystem, letting you read files and glob repository contents directly from GitHub inside any DuckDB query — no manual downloading required.

## URL format

```
gh://owner/repo@ref/path/to/file      -- explicit branch, tag, or commit
gh://owner/repo/path/to/file          -- resolves to the repository default branch
```

Examples:

```
gh://duckdb/duckdb@main/data/csv/test.csv
gh://my-org/my-repo@v2.1.0/data/snapshot.parquet
gh://my-org/private-repo/config/settings.json
```

## Reading files

Any DuckDB function that accepts a file path works transparently with `gh://` URLs:

```sql
-- Query a CSV directly from GitHub
SELECT * FROM read_csv('gh://duckdb/duckdb@main/data/csv/issue2934.csv') LIMIT 10;

-- Read a Parquet file from a tagged release
SELECT count(*) FROM read_parquet('gh://my-org/my-repo@v1.0/data/snapshot.parquet');

-- Read a JSON file
SELECT * FROM read_json('gh://my-org/my-repo@main/config/settings.json');
```

## Globbing

`glob()` and wildcard paths in multi-file readers work with two strategies:

### BFS (single-level wildcards — `*`, `?`, `[…]`)

Uses the GitHub Contents API one directory at a time with segment-by-segment pruning to skip subtrees that can never match the pattern. Efficient when the wildcard depth is shallow or the repo is large.

```sql
-- All CSV files exactly two levels below data/csv/
SELECT * FROM glob('gh://duckdb/duckdb@main/data/csv/*/*.csv');

-- Multi-level single-wildcard traversal
SELECT count(*) FROM glob('gh://duckdb/duckdb@main/data/csv/glob/crawl/d/d00/d10/*/*/*/*.csv');
```

### Trees API (`**` patterns)

Uses a single `GET /git/trees/{sha}?recursive=1` call scoped to the base directory, then filters results client-side. Ideal for open-ended recursive searches.

```sql
-- All CSV files anywhere under data/csv/glob/
SELECT count(*) FROM glob('gh://duckdb/duckdb@main/data/csv/glob/**/*.csv');

-- Use directly in a multi-file reader
SELECT * FROM read_csv('gh://my-org/my-repo@main/data/**/*.csv');
```

## Repository metadata functions

Two table functions expose GitHub repository metadata as SQL rows.

### `gh_repo('<org>/<repo>')` — single repository

Returns one row of metadata for the given repository.

```sql
-- Metadata for a single repo
SELECT name, stargazers_count, language, topics
FROM gh_repo('duckdb/duckdb');

-- Use it like any table — filter, join, aggregate
SELECT description FROM gh_repo('my-org/my-repo')
WHERE archived = false;
```

Pass `'<org>/*'` to expand to **all repositories** for an org or user:

```sql
-- All repos for an org, sorted by stars
SELECT name, stargazers_count, language
FROM gh_repo('duckdb/*')
ORDER BY stargazers_count DESC;

-- Count public repos by language
SELECT language, count(*) AS n
FROM gh_repo('my-org/*')
WHERE NOT "private"
GROUP BY language
ORDER BY n DESC;
```

### `gh_repos((table))` — multiple repositories from a table

A table in-out function that accepts any query returning a single VARCHAR column of `'<org>/<repo>'` strings and returns one metadata row per input row. `'<org>/*'` rows are expanded to all repos for that org.

```sql
-- Fixed list via VALUES
SELECT name, stargazers_count
FROM gh_repos((VALUES ('duckdb/duckdb'), ('duckdb/pg_duckdb'), ('duckdb/duckdb-wasm')))
ORDER BY stargazers_count DESC;

-- From a table column
SELECT r.name, r.language, r.stargazers_count
FROM my_repos, gh_repos((SELECT repo_name FROM my_repos)) r;

-- Mix an org wildcard with specific repos
SELECT name, full_name
FROM gh_repos((VALUES ('my-org/*'), ('other-org/specific-repo')))
ORDER BY name;
```

### Output columns

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR | Repository name |
| `full_name` | VARCHAR | `org/repo` |
| `description` | VARCHAR | Repository description |
| `owner` | VARCHAR | Owner login |
| `private` | BOOLEAN | Whether the repository is private |
| `fork` | BOOLEAN | Whether this is a fork |
| `archived` | BOOLEAN | Whether the repository is archived |
| `disabled` | BOOLEAN | Whether the repository is disabled |
| `visibility` | VARCHAR | `public`, `private`, or `internal` |
| `default_branch` | VARCHAR | Default branch name |
| `language` | VARCHAR | Primary language |
| `license` | VARCHAR | SPDX license identifier |
| `homepage` | VARCHAR | Homepage URL |
| `html_url` | VARCHAR | GitHub URL |
| `topics` | VARCHAR[] | Repository topics |
| `stargazers_count` | BIGINT | Star count |
| `watchers_count` | BIGINT | Watcher count |
| `forks_count` | BIGINT | Fork count |
| `open_issues_count` | BIGINT | Open issue count |
| `size` | BIGINT | Repository size in KB |
| `created_at` | TIMESTAMP | Creation time |
| `updated_at` | TIMESTAMP | Last update time |
| `pushed_at` | TIMESTAMP | Last push time |

## Authentication

Without a token the GitHub API allows 60 unauthenticated requests per hour. To get 5,000 requests per hour, create a secret with a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens):

```sql
CREATE SECRET my_github_token (
    TYPE github,
    TOKEN 'github_pat_...'
);
```

The secret is used automatically for all `gh://` requests in the session. For private repositories a token with `repo` scope is required.

You can also set the token via the `GITHUB_TOKEN` environment variable before starting DuckDB — the extension picks it up automatically.

## Rate limits

When the GitHub API rate limit is exceeded the extension throws a descriptive error that includes the reset time and, when no token is configured, a hint to add one:

```
GitHub API rate limit exceeded. Set a GitHub token via CREATE SECRET (TYPE github, TOKEN '...') for a higher limit. Rate limit resets at 14:32:00.
```

## Building

```sh
git clone --recurse-submodules https://github.com/your-org/duckdb-gh.git
cd duckdb-gh
GEN=ninja make -j$(nproc)
```

Run tests (network tests require a `GITHUB_TOKEN` environment variable):

```sh
build/release/test/unittest "test/sql/gh.test"
GITHUB_TOKEN=$(gh auth token) build/release/test/unittest "test/sql/gh_network.test"
```
