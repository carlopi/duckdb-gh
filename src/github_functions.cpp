#include "github_functions.hpp"
#include "github_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "yyjson.hpp"

#include <cstring>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//===--------------------------------------------------------------------===//
// JSON helpers
//===--------------------------------------------------------------------===//

static Value GetStrVal(yyjson_val *root, const char *key) {
	yyjson_val *v = yyjson_obj_get(root, key);
	if (!v || yyjson_is_null(v) || !yyjson_is_str(v)) {
		return Value(LogicalType::VARCHAR);
	}
	return Value(yyjson_get_str(v));
}

static Value GetBoolVal(yyjson_val *root, const char *key) {
	yyjson_val *v = yyjson_obj_get(root, key);
	if (!v || yyjson_is_null(v) || !yyjson_is_bool(v)) {
		return Value(LogicalType::BOOLEAN);
	}
	return Value::BOOLEAN(yyjson_get_bool(v));
}

static Value GetIntVal(yyjson_val *root, const char *key) {
	yyjson_val *v = yyjson_obj_get(root, key);
	if (!v || yyjson_is_null(v)) {
		return Value(LogicalType::BIGINT);
	}
	return Value::BIGINT(yyjson_get_sint(v));
}

static Value ParseTimestampVal(yyjson_val *v) {
	if (!v || yyjson_is_null(v) || !yyjson_is_str(v)) {
		return Value(LogicalType::TIMESTAMP);
	}
	const char *s = yyjson_get_str(v);
	try {
		// GitHub timestamps are ISO 8601 with "Z" suffix (UTC offset).
		auto ts = Timestamp::FromCString(s, strlen(s), /*use_offset=*/true);
		return Value::TIMESTAMP(ts);
	} catch (...) {
		return Value(LogicalType::TIMESTAMP);
	}
}

//===--------------------------------------------------------------------===//
// Output schema (shared by both functions)
//===--------------------------------------------------------------------===//

static void SetRepoOutputSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"name",      "full_name",          "description",     "owner",             "private",
	         "fork",      "archived",           "disabled",        "visibility",        "default_branch",
	         "language",  "license",            "homepage",        "html_url",          "topics",
	         "stargazers_count", "watchers_count", "forks_count", "open_issues_count", "size",
	         "created_at", "updated_at",        "pushed_at"};

	return_types = {
	    LogicalType::VARCHAR,                   // name
	    LogicalType::VARCHAR,                   // full_name
	    LogicalType::VARCHAR,                   // description
	    LogicalType::VARCHAR,                   // owner
	    LogicalType::BOOLEAN,                   // private
	    LogicalType::BOOLEAN,                   // fork
	    LogicalType::BOOLEAN,                   // archived
	    LogicalType::BOOLEAN,                   // disabled
	    LogicalType::VARCHAR,                   // visibility
	    LogicalType::VARCHAR,                   // default_branch
	    LogicalType::VARCHAR,                   // language
	    LogicalType::VARCHAR,                   // license
	    LogicalType::VARCHAR,                   // homepage
	    LogicalType::VARCHAR,                   // html_url
	    LogicalType::LIST(LogicalType::VARCHAR), // topics
	    LogicalType::BIGINT,                    // stargazers_count
	    LogicalType::BIGINT,                    // watchers_count
	    LogicalType::BIGINT,                    // forks_count
	    LogicalType::BIGINT,                    // open_issues_count
	    LogicalType::BIGINT,                    // size
	    LogicalType::TIMESTAMP,                 // created_at
	    LogicalType::TIMESTAMP,                 // updated_at
	    LogicalType::TIMESTAMP,                 // pushed_at
	};
}

//===--------------------------------------------------------------------===//
// Glob expansion: list all repos for an owner
//
// Input format: "owner/*" — owner is literal, repo part must be exactly "*".
// Returns a vector of "owner/repo" strings.
//
// Tries /orgs/{owner}/repos first; on 404 falls back to /users/{owner}/repos.
// Paginates with per_page=100 until a page returns fewer than 100 entries.
//===--------------------------------------------------------------------===//

static vector<string> ListAllRepos(const string &owner, const string &token, optional_ptr<FileOpener> opener) {
	vector<string> result;

	// Determine listing endpoint: orgs (first) or users (fallback on 404)
	string base_url;
	{
		bool not_found = false;
		string probe = "https://api.github.com/orgs/" + owner + "/repos?per_page=1";
		GithubFileSystem::CallAPI(probe, token, opener, &not_found);
		base_url = not_found ? ("https://api.github.com/users/" + owner + "/repos")
		                     : ("https://api.github.com/orgs/" + owner + "/repos");
	}

	for (int page = 1;; page++) {
		string url = base_url + "?per_page=100&page=" + std::to_string(page);
		bool not_found = false;
		string body = GithubFileSystem::CallAPI(url, token, opener, &not_found);
		if (not_found || body.empty()) {
			break;
		}

		yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
		if (!doc) {
			break;
		}
		yyjson_val *arr = yyjson_doc_get_root(doc);
		if (!yyjson_is_arr(arr)) {
			yyjson_doc_free(doc);
			break;
		}

		idx_t count = yyjson_arr_size(arr);
		yyjson_val *item;
		size_t idx, max;
		yyjson_arr_foreach(arr, idx, max, item) {
			yyjson_val *name_val = yyjson_obj_get(item, "name");
			if (name_val && yyjson_is_str(name_val)) {
				result.push_back(owner + "/" + yyjson_get_str(name_val));
			}
		}
		yyjson_doc_free(doc);

		if (count < 100) {
			break; // last page
		}
	}

	return result;
}

// Parse "owner/repo_part" and expand if repo_part == "*".
// Returns the full list of "owner/repo" strings to fetch.
static vector<string> ExpandRepoPattern(const string &pattern, const string &token, optional_ptr<FileOpener> opener) {
	auto slash = pattern.find('/');
	if (slash == string::npos) {
		throw InvalidInputException("gh_repo: expected '<org>/<repo>' or '<org>/*', got '%s'", pattern);
	}
	string owner = pattern.substr(0, slash);
	string repo_part = pattern.substr(slash + 1);

	if (repo_part == "*") {
		return ListAllRepos(owner, token, opener);
	}
	// Reject partial globs like 'owner/prefix*' — only 'owner/*' is supported.
	if (repo_part.find_first_of("*?[") != string::npos) {
		throw InvalidInputException(
		    "gh_repo: only '<org>/*' is supported as a wildcard pattern; got '%s'. "
		    "Use '<org>/*' to list all repos for an org.",
		    pattern);
	}
	return {pattern};
}

//===--------------------------------------------------------------------===//
// Core: fetch one repo and populate one output row
//===--------------------------------------------------------------------===//

static void FetchAndParseRepo(const string &repo_str, const string &token, optional_ptr<FileOpener> opener,
                              DataChunk &output, idx_t row) {
	auto slash = repo_str.find('/');
	if (slash == string::npos) {
		throw InvalidInputException("gh_repo: expected '<org>/<repo>', got '%s'", repo_str);
	}

	string url = "https://api.github.com/repos/" + repo_str.substr(0, slash) + "/" + repo_str.substr(slash + 1);
	bool not_found = false;
	string body = GithubFileSystem::CallAPI(url, token, opener, &not_found);
	if (not_found || body.empty()) {
		throw IOException("gh_repo: repository '%s' not found", repo_str);
	}

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (!doc) {
		throw IOException("gh_repo: failed to parse JSON response for '%s'", repo_str);
	}
	yyjson_val *root = yyjson_doc_get_root(doc);

	output.SetValue(0, row, GetStrVal(root, "name"));
	output.SetValue(1, row, GetStrVal(root, "full_name"));
	output.SetValue(2, row, GetStrVal(root, "description"));
	{
		yyjson_val *owner_obj = yyjson_obj_get(root, "owner");
		Value owner_val(LogicalType::VARCHAR);
		if (owner_obj && !yyjson_is_null(owner_obj)) {
			yyjson_val *login = yyjson_obj_get(owner_obj, "login");
			if (login && yyjson_is_str(login)) {
				owner_val = Value(yyjson_get_str(login));
			}
		}
		output.SetValue(3, row, owner_val);
	}
	output.SetValue(4, row, GetBoolVal(root, "private"));
	output.SetValue(5, row, GetBoolVal(root, "fork"));
	output.SetValue(6, row, GetBoolVal(root, "archived"));
	output.SetValue(7, row, GetBoolVal(root, "disabled"));
	output.SetValue(8, row, GetStrVal(root, "visibility"));
	output.SetValue(9, row, GetStrVal(root, "default_branch"));
	output.SetValue(10, row, GetStrVal(root, "language"));
	{
		yyjson_val *license_obj = yyjson_obj_get(root, "license");
		Value license_val(LogicalType::VARCHAR);
		if (license_obj && !yyjson_is_null(license_obj)) {
			yyjson_val *spdx = yyjson_obj_get(license_obj, "spdx_id");
			if (spdx && yyjson_is_str(spdx)) {
				license_val = Value(yyjson_get_str(spdx));
			}
		}
		output.SetValue(11, row, license_val);
	}
	output.SetValue(12, row, GetStrVal(root, "homepage"));
	output.SetValue(13, row, GetStrVal(root, "html_url"));
	{
		vector<Value> topics;
		yyjson_val *topics_arr = yyjson_obj_get(root, "topics");
		if (topics_arr && yyjson_is_arr(topics_arr)) {
			yyjson_val *item;
			size_t idx, max;
			yyjson_arr_foreach(topics_arr, idx, max, item) {
				const char *s = yyjson_get_str(item);
				if (s) {
					topics.emplace_back(Value(s));
				}
			}
		}
		output.SetValue(14, row, Value::LIST(LogicalType::VARCHAR, topics));
	}
	output.SetValue(15, row, GetIntVal(root, "stargazers_count"));
	output.SetValue(16, row, GetIntVal(root, "watchers_count"));
	output.SetValue(17, row, GetIntVal(root, "forks_count"));
	output.SetValue(18, row, GetIntVal(root, "open_issues_count"));
	output.SetValue(19, row, GetIntVal(root, "size"));
	output.SetValue(20, row, ParseTimestampVal(yyjson_obj_get(root, "created_at")));
	output.SetValue(21, row, ParseTimestampVal(yyjson_obj_get(root, "updated_at")));
	output.SetValue(22, row, ParseTimestampVal(yyjson_obj_get(root, "pushed_at")));

	yyjson_doc_free(doc);
}

//===--------------------------------------------------------------------===//
// gh_repo('<org>/<repo>' | '<org>/*') — single VARCHAR, regular table function
//
// SQL usage:
//   SELECT * FROM gh_repo('duckdb/duckdb');
//   SELECT name, stargazers_count FROM gh_repo('my-org/*') ORDER BY stargazers_count DESC;
//
// '<org>/<repo>'  — fetches metadata for exactly one repository (1 row).
// '<org>/*'       — lists all repositories for the org/user and fetches
//                   metadata for each one (N rows, one per repo).
//
// Partial wildcards such as '<org>/prefix*' are not supported and will
// raise an InvalidInputException.
//===--------------------------------------------------------------------===//

struct GithubRepoBindData : public TableFunctionData {
	string token;
	string pattern; // raw input, may be "owner/*"
};

struct GithubRepoScanState : public GlobalTableFunctionState {
	bool expanded = false;
	vector<string> repos; // after expansion
	idx_t current = 0;
};

static unique_ptr<FunctionData> GithubRepoBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	SetRepoOutputSchema(return_types, names);
	ClientContextFileOpener opener(context);
	auto bind_data = make_uniq<GithubRepoBindData>();
	bind_data->token = GithubFileSystem::GetToken(&opener);
	bind_data->pattern = input.inputs[0].ToString();
	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> GithubRepoInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GithubRepoScanState>();
}

static void GithubRepoScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<GithubRepoBindData>();
	auto &state = data_p.global_state->Cast<GithubRepoScanState>();

	// Expand the pattern once on first call
	if (!state.expanded) {
		state.expanded = true;
		ClientContextFileOpener opener(context);
		state.repos = ExpandRepoPattern(bind.pattern, bind.token, &opener);
	}

	if (state.current >= state.repos.size()) {
		output.SetCardinality(0);
		return;
	}

	ClientContextFileOpener opener(context);
	idx_t out_row = 0;
	while (state.current < state.repos.size() && out_row < STANDARD_VECTOR_SIZE) {
		FetchAndParseRepo(state.repos[state.current++], bind.token, &opener, output, out_row++);
	}
	output.SetCardinality(out_row);
}

//===--------------------------------------------------------------------===//
// gh_repos((table)) — table in-out, one row per input '<org>/<repo>' or '<org>/*'
//
// SQL usage:
//   SELECT * FROM gh_repos((VALUES ('duckdb/duckdb'), ('duckdb/pg_duckdb')));
//   SELECT * FROM gh_repos((SELECT repo_name FROM my_repos));
//   SELECT * FROM gh_repos((VALUES ('my-org/*'), ('other-org/specific-repo')));
//
// Accepts a single-column VARCHAR table.  Each input row may be:
//   '<org>/<repo>'  — fetches metadata for exactly one repository.
//   '<org>/*'       — expands to all repositories for the org/user, then
//                     fetches metadata for each one.
//
// HAVE_MORE_OUTPUT is returned when a single input chunk (after glob
// expansion) produces more rows than STANDARD_VECTOR_SIZE, so the caller
// receives all results across multiple output chunks.
//===--------------------------------------------------------------------===//

struct GithubReposBindData : public TableFunctionData {
	string token;
};

struct GithubReposGlobalState : public GlobalTableFunctionState {
	vector<string> pending;      // expanded repos waiting to be fetched
	idx_t current = 0;
	bool chunk_processed = false; // whether current input chunk was expanded
};

static unique_ptr<FunctionData> GithubReposBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.input_table_types.size() != 1 || input.input_table_types[0] != LogicalType::VARCHAR) {
		throw InvalidInputException("gh_repos expects a table with a single VARCHAR column ('<org>/<repo>' or '<org>/*')");
	}
	SetRepoOutputSchema(return_types, names);
	ClientContextFileOpener opener(context);
	auto bind_data = make_uniq<GithubReposBindData>();
	bind_data->token = GithubFileSystem::GetToken(&opener);
	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> GithubReposInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GithubReposGlobalState>();
}

static OperatorResultType GithubReposInOut(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                           DataChunk &output) {
	auto &bind = data.bind_data->Cast<GithubReposBindData>();
	auto &state = data.global_state->Cast<GithubReposGlobalState>();
	ClientContextFileOpener opener(context.client);

	// First call for this input chunk: expand all patterns into pending list
	if (!state.chunk_processed) {
		state.pending.clear();
		state.current = 0;
		state.chunk_processed = true;
		for (idx_t i = 0; i < input.size(); i++) {
			auto expanded = ExpandRepoPattern(input.data[0].GetValue(i).ToString(), bind.token, &opener);
			for (auto &r : expanded) {
				state.pending.push_back(std::move(r));
			}
		}
	}

	// Emit up to STANDARD_VECTOR_SIZE rows from the pending list
	idx_t out_row = 0;
	while (state.current < state.pending.size() && out_row < STANDARD_VECTOR_SIZE) {
		FetchAndParseRepo(state.pending[state.current++], bind.token, &opener, output, out_row++);
	}
	output.SetCardinality(out_row);

	if (state.current < state.pending.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT; // still draining this chunk
	}
	state.chunk_processed = false; // ready for the next input chunk
	return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// gh_issues('<org>/<repo>') — paginated issue listing
//
// SQL usage:
//   SELECT * FROM gh_issues('duckdb/duckdb');
//   SELECT number, title FROM gh_issues('duckdb/duckdb', state := 'closed');
//   SELECT * FROM gh_issues('duckdb/duckdb', state := 'all');
//
// Returns one row per issue (pull requests are excluded).
// Paginates automatically — one API call per 100 issues.
// Default state is 'open'. Valid values: 'open', 'closed', 'all'.
//===--------------------------------------------------------------------===//

static void SetIssueOutputSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"number",     "title",   "state",      "state_reason", "body",       "user",
	         "labels",     "assignees", "milestone", "locked",       "comments",   "created_at",
	         "updated_at", "closed_at", "html_url"};

	return_types = {
	    LogicalType::BIGINT,                    // number
	    LogicalType::VARCHAR,                   // title
	    LogicalType::VARCHAR,                   // state
	    LogicalType::VARCHAR,                   // state_reason (nullable)
	    LogicalType::VARCHAR,                   // body (nullable)
	    LogicalType::VARCHAR,                   // user (login)
	    LogicalType::LIST(LogicalType::VARCHAR), // labels
	    LogicalType::LIST(LogicalType::VARCHAR), // assignees
	    LogicalType::VARCHAR,                   // milestone (nullable)
	    LogicalType::BOOLEAN,                   // locked
	    LogicalType::BIGINT,                    // comments
	    LogicalType::TIMESTAMP,                 // created_at
	    LogicalType::TIMESTAMP,                 // updated_at
	    LogicalType::TIMESTAMP,                 // closed_at (nullable)
	    LogicalType::VARCHAR,                   // html_url
	};
}

static void ParseIssueRow(yyjson_val *item, DataChunk &output, idx_t row) {
	output.SetValue(0, row, GetIntVal(item, "number"));
	output.SetValue(1, row, GetStrVal(item, "title"));
	output.SetValue(2, row, GetStrVal(item, "state"));
	output.SetValue(3, row, GetStrVal(item, "state_reason"));
	output.SetValue(4, row, GetStrVal(item, "body"));

	// user.login
	{
		yyjson_val *user = yyjson_obj_get(item, "user");
		Value user_val(LogicalType::VARCHAR);
		if (user && !yyjson_is_null(user)) {
			yyjson_val *login = yyjson_obj_get(user, "login");
			if (login && yyjson_is_str(login)) {
				user_val = Value(yyjson_get_str(login));
			}
		}
		output.SetValue(5, row, user_val);
	}

	// labels[].name
	{
		vector<Value> labels;
		yyjson_val *arr = yyjson_obj_get(item, "labels");
		if (arr && yyjson_is_arr(arr)) {
			yyjson_val *lbl;
			size_t i, m;
			yyjson_arr_foreach(arr, i, m, lbl) {
				yyjson_val *name = yyjson_obj_get(lbl, "name");
				if (name && yyjson_is_str(name)) {
					labels.emplace_back(Value(yyjson_get_str(name)));
				}
			}
		}
		output.SetValue(6, row, Value::LIST(LogicalType::VARCHAR, labels));
	}

	// assignees[].login
	{
		vector<Value> assignees;
		yyjson_val *arr = yyjson_obj_get(item, "assignees");
		if (arr && yyjson_is_arr(arr)) {
			yyjson_val *a;
			size_t i, m;
			yyjson_arr_foreach(arr, i, m, a) {
				yyjson_val *login = yyjson_obj_get(a, "login");
				if (login && yyjson_is_str(login)) {
					assignees.emplace_back(Value(yyjson_get_str(login)));
				}
			}
		}
		output.SetValue(7, row, Value::LIST(LogicalType::VARCHAR, assignees));
	}

	// milestone.title
	{
		yyjson_val *ms = yyjson_obj_get(item, "milestone");
		Value ms_val(LogicalType::VARCHAR);
		if (ms && !yyjson_is_null(ms)) {
			yyjson_val *title = yyjson_obj_get(ms, "title");
			if (title && yyjson_is_str(title)) {
				ms_val = Value(yyjson_get_str(title));
			}
		}
		output.SetValue(8, row, ms_val);
	}

	output.SetValue(9, row, GetBoolVal(item, "locked"));
	output.SetValue(10, row, GetIntVal(item, "comments"));
	output.SetValue(11, row, ParseTimestampVal(yyjson_obj_get(item, "created_at")));
	output.SetValue(12, row, ParseTimestampVal(yyjson_obj_get(item, "updated_at")));
	output.SetValue(13, row, ParseTimestampVal(yyjson_obj_get(item, "closed_at")));
	output.SetValue(14, row, GetStrVal(item, "html_url"));
}

struct GithubIssuesBindData : public TableFunctionData {
	string token;
	string owner;
	string repo;
	string state; // "open", "closed", or "all"
};

struct GithubIssuesScanState : public GlobalTableFunctionState {
	int page = 1;
	bool done = false;
};

static unique_ptr<FunctionData> GithubIssuesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	SetIssueOutputSchema(return_types, names);

	auto repo_str = input.inputs[0].ToString();
	auto slash = repo_str.find('/');
	if (slash == string::npos) {
		throw InvalidInputException("gh_issues: expected '<org>/<repo>', got '%s'", repo_str);
	}
	if (repo_str.find_first_of("*?[") != string::npos) {
		throw InvalidInputException("gh_issues: glob patterns are not supported; expected '<org>/<repo>'");
	}

	string state_val = "open";
	auto it = input.named_parameters.find("state");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		state_val = it->second.ToString();
		if (state_val != "open" && state_val != "closed" && state_val != "all") {
			throw InvalidInputException(
			    "gh_issues: invalid state '%s'; expected 'open', 'closed', or 'all'", state_val);
		}
	}

	ClientContextFileOpener opener(context);
	auto bind_data = make_uniq<GithubIssuesBindData>();
	bind_data->token = GithubFileSystem::GetToken(&opener);
	bind_data->owner = repo_str.substr(0, slash);
	bind_data->repo = repo_str.substr(slash + 1);
	bind_data->state = state_val;
	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> GithubIssuesInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GithubIssuesScanState>();
}

static void GithubIssuesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<GithubIssuesBindData>();
	auto &state = data_p.global_state->Cast<GithubIssuesScanState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	ClientContextFileOpener opener(context);
	string url = "https://api.github.com/repos/" + bind.owner + "/" + bind.repo +
	             "/issues?state=" + bind.state + "&per_page=100&page=" + std::to_string(state.page);

	bool not_found = false;
	string body;
	try {
		body = GithubFileSystem::CallAPI(url, bind.token, &opener, &not_found);
	} catch (IOException &e) {
		// GitHub returns 422 Unprocessable when the page offset exceeds its internal
		// limit for this endpoint. Re-raise with actionable guidance.
		string msg(e.what());
		if (msg.find("Unprocessable") != string::npos) {
			throw IOException(
			    "gh_issues: GitHub REST API pagination limit reached for '%s/%s' (state='%s'). "
			    "The REST API cannot return results beyond a certain offset. "
			    "Try narrowing the result set: use state='open' or state='closed' instead of 'all'.",
			    bind.owner, bind.repo, bind.state);
		}
		throw;
	}
	if (not_found) {
		throw IOException("gh_issues: repository '%s/%s' not found", bind.owner, bind.repo);
	}
	if (body.empty()) {
		state.done = true;
		output.SetCardinality(0);
		return;
	}

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (!doc) {
		state.done = true;
		output.SetCardinality(0);
		return;
	}
	yyjson_val *arr = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(arr)) {
		yyjson_doc_free(doc);
		state.done = true;
		output.SetCardinality(0);
		return;
	}

	idx_t api_count = yyjson_arr_size(arr);
	idx_t out_row = 0;
	yyjson_val *item;
	size_t idx, max;
	yyjson_arr_foreach(arr, idx, max, item) {
		// The issues endpoint returns pull requests too; skip them.
		yyjson_val *pr = yyjson_obj_get(item, "pull_request");
		if (pr && !yyjson_is_null(pr)) {
			continue;
		}
		ParseIssueRow(item, output, out_row++);
	}
	output.SetCardinality(out_row);
	yyjson_doc_free(doc);

	if (api_count < 100) {
		state.done = true;
	} else {
		state.page++;
	}
}

//===--------------------------------------------------------------------===//
// Factories
//===--------------------------------------------------------------------===//

TableFunction GithubRepoFunction() {
	TableFunction fn("gh_repo", {LogicalType::VARCHAR}, GithubRepoScan, GithubRepoBind);
	fn.init_global = GithubRepoInit;
	return fn;
}

TableFunction GithubReposFunction() {
	TableFunction fn("gh_repos", {LogicalType::TABLE}, nullptr, GithubReposBind);
	fn.in_out_function = GithubReposInOut;
	fn.init_global = GithubReposInit;
	return fn;
}

TableFunction GithubIssuesFunction() {
	TableFunction fn("gh_issues", {LogicalType::VARCHAR}, GithubIssuesScan, GithubIssuesBind);
	fn.init_global = GithubIssuesInit;
	fn.named_parameters["state"] = LogicalType::VARCHAR;
	return fn;
}

} // namespace duckdb
