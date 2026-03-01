# Extension TODOs

Improvements and open questions for the `gh://` filesystem extension.

---

## 1 — Verify `ref=HEAD` behaviour when a branch is literally named "HEAD"

**Location:** `src/github_filesystem.cpp` — `GithubFileSystem::OpenFile` (and `FileExists`,
`GithubGlobResult`, `GithubTreesGlobResult`)

When no ref is supplied in the URL (e.g. `gh://duckdb/duckdb/README.md`), the extension passes
`ref=HEAD` to the GitHub Contents API instead of calling `GET /repos/{owner}/{repo}` to resolve
the default branch.  This is cheaper (saves one round-trip) and works because GitHub resolves the
symbolic `HEAD` to the default branch automatically.

**Open question:** if a repository has a branch literally named `"HEAD"`, GitHub might prefer the
exact branch match over the symbolic ref, silently returning the wrong content.

**How to verify:** create a test repo that has a non-default branch called `HEAD`, then compare:

```sql
SELECT md5(string_agg(content, ''))
FROM read_csv('gh://org/repo/file.csv', …);          -- should read default branch

SELECT md5(string_agg(content, ''))
FROM read_csv('gh://org/repo@HEAD/file.csv', …);     -- ambiguous: branch or symbolic?
```

If the hashes differ, replace the hard-coded `"HEAD"` with a call to `ResolveDefaultBranch()`.

---

## 2 — BFS: reuse child tree SHAs from Contents API responses

**Location:** `src/github_filesystem.cpp` — `GithubGlobResult::ExpandNextPath`

Each Contents API response already includes the `"sha"` field for every directory entry — that SHA
is the git tree SHA for that subdirectory.  Currently the BFS ignores these SHAs and re-fetches
each queued directory via another Contents API call (one call per directory).

Instead the BFS could push `(path, sha)` pairs onto `pending_dirs` and use
`GET /git/trees/{sha}` (non-recursive, no page limit) for each queued entry.  The trees endpoint
returns a flat list of entries at roughly the same cost as a Contents API call, but:

- avoids GitHub's 1,000-item per-page limit on the Contents API,
- returns the same SHA fields, enabling the same optimisation to chain transitively.

On its own this does not reduce the total request count (still one call per visited directory),
but it is a prerequisite for item 3.

---

## 3 — BFS: batch sibling directories via the GitHub GraphQL API

**Location:** `src/github_filesystem.cpp` — `GlobFilesExtended` / `GithubGlobResult`

The GitHub REST API has no batch endpoint — each `/contents/{path}` (or `/git/trees/{sha}`) call
fetches exactly one directory.  The GraphQL API removes this restriction: multiple tree lookups can
be aliased inside a single `POST https://api.github.com/graphql`, collapsing an entire BFS level
into one round-trip.

**Example:** after visiting `d10` and discovering children `d20`, `d21`, `d22`, instead of three
separate REST calls the BFS could issue one GraphQL request:

```graphql
query {
  repository(owner: "duckdb", name: "duckdb") {
    d20: object(expression: "main:.../d10/d20") { ...on Tree { entries { name type oid } } }
    d21: object(expression: "main:.../d10/d21") { ...on Tree { entries { name type oid } } }
    d22: object(expression: "main:.../d10/d22") { ...on Tree { entries { name type oid } } }
  }
}
```

This reduces BFS to **one request per depth level** instead of one per directory.  For the
`data/csv/glob/crawl/d/d00/d10/*/*/*/*.csv` fixture the 16 REST calls would become ~4 GraphQL
calls.

**Implementation considerations:**

- Requires `POST` to `https://api.github.com/graphql` with a JSON body (not a plain GET).
- The GraphQL endpoint **always requires authentication** — no unauthenticated access.
- The query must be built dynamically, one alias per pending directory in the current BFS frontier.
- Response parsing changes from a flat JSON array to a map of aliased objects.
- GraphQL and REST API rate limits are tracked separately by GitHub.

---

## 4 — gh_repo / gh_repos: batch metadata fetching

**Location:** `src/github_functions.cpp` — `FetchAndParseRepo`, `GithubRepoScan`, `GithubReposInOut`

Both `gh_repo('owner/repo')` and `gh_repos((table))` currently issue one `GET /repos/{owner}/{repo}`
request per row.  For large inputs this is slow and burns API rate limit quota quickly.

**Potential batch approaches:**

### GitHub GraphQL API

A single `POST https://api.github.com/graphql` can alias multiple repository lookups:

```graphql
query {
  r0: repository(owner: "duckdb",   name: "duckdb")    { name stargazerCount ... }
  r1: repository(owner: "duckdb",   name: "pg_duckdb") { name stargazerCount ... }
  r2: repository(owner: "carlopi",  name: "duckdb-gh")  { name stargazerCount ... }
}
```

This collapses N repos into a single round-trip, subject only to GraphQL query complexity limits
(GitHub allows up to ~100 aliases per query in practice).  The response is a JSON object keyed by
alias; rows can be extracted in the same order as the aliases.

**Implementation considerations:**
- Requires building a dynamic GraphQL query string (one alias per repo).
- Requires `POST` with a `{"query": "..."}` JSON body — `CallAPI` currently only does `GET`.
- Response parsing: navigate `data.r0`, `data.r1`, … rather than a flat array.
- The GraphQL endpoint **always requires authentication**; unauthenticated calls are rejected.
- GraphQL and REST rate limits are tracked separately by GitHub.
- Batch size should be capped (e.g. 100 aliases per request) with multiple requests for larger inputs.

### REST: no true batch endpoint exists

The GitHub REST API has no `POST /repos/batch` or equivalent.  The only REST-level optimisation
is parallelising independent `GET /repos/{owner}/{repo}` calls, which DuckDB's parallel execution
context could support if the in-out / scan functions are made thread-safe.
