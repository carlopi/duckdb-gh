#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// gh_repo('owner/repo') — single VARCHAR argument
TableFunction GithubRepoFunction();

// gh_repos((SELECT ...)) — table in-out, one row per input 'owner/repo'
TableFunction GithubReposFunction();

// gh_issues('owner/repo') — paginated issue listing (excludes pull requests)
TableFunction GithubIssuesFunction();

} // namespace duckdb
