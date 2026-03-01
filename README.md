# duckdb-gh

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
build/release/test/unittest "test/sql/quack.test"
GITHUB_TOKEN=$(gh auth token) build/release/test/unittest "test/sql/quack_gh_network.test"
```
