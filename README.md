# duckdb-gh

> **Experimental:** this extension is in early development. APIs, behaviour, and URL formats may change without notice. Use with caution.

A [DuckDB](https://duckdb.org) extension that lets you query GitHub directly from SQL — read files, glob repository contents, and fetch repository and issue metadata without downloading anything manually.

## Quick start

```sql
-- Read a CSV straight from GitHub
SELECT * FROM read_csv('gh://duckdb/duckdb@main/data/csv/issue2934.csv') LIMIT 5;

-- Star counts for all DuckDB repos
SELECT name, stargazers_count, language
FROM gh_repo('duckdb/*')
ORDER BY stargazers_count DESC;

-- Open issues for a repo
SELECT number, title, labels
FROM gh_issues('duckdb/duckdb')
ORDER BY number DESC
LIMIT 20;

-- Recursively read all CSVs under a directory
SELECT count(*) FROM read_csv('gh://duckdb/duckdb@main/data/csv/glob/**/*.csv');
```

## Authentication

Without a token the GitHub API allows 60 requests/hour. Add a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) to get 5,000/hour:

```sql
CREATE SECRET my_github_token (
    TYPE github,
    TOKEN 'github_pat_...'
);
```

Or set `GITHUB_TOKEN` in your environment before starting DuckDB — the extension picks it up automatically. Private repositories require a token with `repo` scope.

## Reading files

Any DuckDB function that accepts a file path works with `gh://` URLs:

```
gh://owner/repo@ref/path/to/file      -- explicit branch, tag, or commit
gh://owner/repo/path/to/file          -- default branch
```

```sql
SELECT * FROM read_csv('gh://duckdb/duckdb@main/data/csv/issue2934.csv') LIMIT 10;
SELECT count(*) FROM read_parquet('gh://my-org/my-repo@v1.0/data/snapshot.parquet');
SELECT * FROM read_json('gh://my-org/my-repo@main/config/settings.json');
```

## Globbing

```sql
-- All CSV files under a directory (recursive)
SELECT * FROM glob('gh://duckdb/duckdb@main/data/csv/glob/**/*.csv');

-- Single-level wildcard
SELECT * FROM glob('gh://duckdb/duckdb@main/data/csv/*/*.csv');

-- Use globs directly in multi-file readers
SELECT count(*) FROM read_csv('gh://duckdb/duckdb@main/data/**/*.csv');
```

## Repository metadata — `gh_repo` / `gh_repos`

```sql
-- Single repo
SELECT name, stargazers_count, language, topics
FROM gh_repo('duckdb/duckdb');

-- All repos for an org
SELECT name, stargazers_count, language
FROM gh_repo('duckdb/*')
ORDER BY stargazers_count DESC;

-- Fixed list of repos
SELECT name, stargazers_count
FROM gh_repos((VALUES ('duckdb/duckdb'), ('duckdb/pg_duckdb'), ('duckdb/duckdb-wasm')))
ORDER BY stargazers_count DESC;

-- From a table
SELECT r.name, r.language
FROM gh_repos((SELECT repo_name FROM my_repos)) r;
```

Columns: `name`, `full_name`, `description`, `owner`, `private`, `fork`, `archived`, `disabled`, `visibility`, `default_branch`, `language`, `license`, `homepage`, `html_url`, `topics`, `stargazers_count`, `watchers_count`, `forks_count`, `open_issues_count`, `size`, `created_at`, `updated_at`, `pushed_at`.

## Issues — `gh_issues`

```sql
-- Open issues (default)
SELECT number, title, user, labels
FROM gh_issues('duckdb/duckdb');

-- Closed issues
SELECT number, title, closed_at
FROM gh_issues('duckdb/duckdb', state := 'closed')
ORDER BY closed_at DESC;

-- All issues
SELECT state, count(*) FROM gh_issues('duckdb/duckdb', state := 'all')
GROUP BY state;
```

Columns: `number`, `title`, `state`, `state_reason`, `body`, `user`, `labels`, `assignees`, `milestone`, `locked`, `comments`, `created_at`, `updated_at`, `closed_at`, `html_url`.

## Building

```sh
git clone --recurse-submodules https://github.com/your-org/duckdb-gh.git
cd duckdb-gh
GEN=ninja make -j$(nproc)
```

```sh
build/release/test/unittest "test/sql/gh.test"
GITHUB_TOKEN=$(gh auth token) build/release/test/unittest "test/sql/gh_network.test"
```
