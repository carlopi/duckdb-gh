# Ideas: GitHub API endpoints as SQL table functions

All endpoints paginate with `?page=N&per_page=100` (max 100 per page) and share
the authentication mechanism already in place (`CREATE SECRET TYPE github` /
`GITHUB_TOKEN` env var).

---

## Implemented

### ✅ Repository metadata — `gh_repo` / `gh_repos`

`GET /repos/{owner}/{repo}` — single repo or org-wide expansion.

### ✅ Issues — `gh_issues`

`GET /repos/{owner}/{repo}/issues` — paginated, PRs excluded, `state` filter.

**Known limitation:** GitHub's REST API has a hard pagination offset limit for
this endpoint. Repos with very large issue counts (e.g. >10k closed issues) will
hit a 422 error before all results are returned. The GraphQL API (cursor-based)
does not have this restriction and is the correct long-term fix.

---

## Priority 1 — most useful for SQL analytics

### Pull Requests — `GET /repos/{owner}/{repo}/pulls`

Key fields: `number`, `title`, `state`, `draft`, `user.login`, `created_at`,
`merged_at`, `closed_at`, `labels[].name`, `requested_reviewers[].login`,
`additions`, `deletions`, `changed_files`, `review_comments`

Same pagination limit caveat as issues applies.

### Commits — `GET /repos/{owner}/{repo}/commits`

Key fields: `sha`, `author.name`, `author.date`, `committer.date`, `message`,
`stats.additions`, `stats.deletions`, `parents[].sha`

Great for time-series analysis — commit frequency, author activity, churn over time.

### Releases — `GET /repos/{owner}/{repo}/releases`

Key fields: `tag_name`, `name`, `draft`, `prerelease`, `author.login`,
`published_at`, `assets[].name`, `assets[].size`, `assets[].download_count`

Download counts per asset make this particularly interesting for analytics.

---

## Priority 2 — useful, lower complexity

### Contributors — `GET /repos/{owner}/{repo}/contributors`

Key fields: `login`, `contributions`

Simple but useful for leaderboards. Note: results are cached by GitHub and may lag.

### Branches & Tags — `GET /repos/{owner}/{repo}/branches` / `/tags`

Key fields: `name`, `commit.sha`, `protected`

### Stargazers — `GET /repos/{owner}/{repo}/stargazers`

Key fields: `login`, `starred_at`

Requires `Accept: application/vnd.github.v3.star+json` to get timestamps.
Star history over time — useful for growth analysis.

---

## Priority 3 — specialised

### Workflow runs — `GET /repos/{owner}/{repo}/actions/runs`

Key fields: `name`, `status`, `conclusion`, `head_branch`, `head_sha`, `event`,
`actor.login`, `created_at`, `updated_at`, `run_number`

CI/CD metrics, flakiness analysis, build duration trends. Requires authentication.

### Workflow jobs — `GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs`

Key fields: `name`, `status`, `conclusion`, `started_at`, `completed_at`,
`steps[].name`, `steps[].conclusion`, `steps[].duration`

Finer-grained than runs — useful for identifying slow or failing steps.

### Deployments — `GET /repos/{owner}/{repo}/deployments`

Key fields: `environment`, `ref`, `sha`, `creator.login`, `created_at`,
`statuses[].state`

### Security alerts — `GET /repos/{owner}/{repo}/dependabot/alerts`, `/code-scanning/alerts`

Key fields: `rule.severity`, `state`, `created_at`. Needs `security_events` scope.

### Search — `GET /search/repositories`, `/search/issues`, `/search/code`, `/search/commits`

Separate rate limit (10 req/min authenticated). Results capped at 1,000 per query.
Useful for ecosystem-wide queries — e.g. all repos with >1k stars using a given topic.

---

## Rate limits

| API family  | Unauthenticated | Authenticated     |
|-------------|-----------------|-------------------|
| Core REST   | 60 req/hour     | 5,000 req/hour    |
| Search      | 10 req/min      | 30 req/min        |
| GraphQL     | —               | 5,000 points/hour |
