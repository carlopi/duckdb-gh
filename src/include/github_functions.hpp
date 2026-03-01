#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Returns the configured TableFunction for gh_repo().
TableFunction GithubRepoFunction();

} // namespace duckdb
