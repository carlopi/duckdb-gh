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
	names = {"name",      "full_name",        "description",   "owner",             "private",
	         "fork",      "archived",         "disabled",      "visibility",        "default_branch",
	         "language",  "license",          "homepage",      "html_url",          "topics",
	         "stargazers_count", "watchers_count", "forks_count", "open_issues_count", "size",
	         "created_at", "updated_at",      "pushed_at"};

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
// Core: fetch one repo and populate one output row
//===--------------------------------------------------------------------===//

static void FetchAndParseRepo(const string &repo_str, const string &token, optional_ptr<FileOpener> opener,
                              DataChunk &output, idx_t row) {
	auto slash = repo_str.find('/');
	if (slash == string::npos) {
		throw InvalidInputException("gh_repo: expected 'owner/repo', got '%s'", repo_str);
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
// gh_repo('owner/repo') — single VARCHAR, regular table function
//===--------------------------------------------------------------------===//

struct GithubRepoBindData : public TableFunctionData {
	string token;
	string repo_str;
};

struct GithubRepoScanState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> GithubRepoBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	SetRepoOutputSchema(return_types, names);
	ClientContextFileOpener opener(context);
	auto bind_data = make_uniq<GithubRepoBindData>();
	bind_data->token = GithubFileSystem::GetToken(&opener);
	bind_data->repo_str = input.inputs[0].ToString();
	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> GithubRepoInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GithubRepoScanState>();
}

static void GithubRepoScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<GithubRepoBindData>();
	auto &state = data_p.global_state->Cast<GithubRepoScanState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
	ClientContextFileOpener opener(context);
	FetchAndParseRepo(bind.repo_str, bind.token, &opener, output, 0);
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// gh_repos((table)) — table in-out, one row per input 'owner/repo'
//===--------------------------------------------------------------------===//

struct GithubReposBindData : public TableFunctionData {
	string token;
};

static unique_ptr<FunctionData> GithubReposBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.input_table_types.size() != 1 || input.input_table_types[0] != LogicalType::VARCHAR) {
		throw InvalidInputException("gh_repos expects a table with a single VARCHAR column ('owner/repo')");
	}
	SetRepoOutputSchema(return_types, names);
	ClientContextFileOpener opener(context);
	auto bind_data = make_uniq<GithubReposBindData>();
	bind_data->token = GithubFileSystem::GetToken(&opener);
	return bind_data;
}

static OperatorResultType GithubReposInOut(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                           DataChunk &output) {
	auto &bind = data.bind_data->Cast<GithubReposBindData>();
	ClientContextFileOpener opener(context.client);
	idx_t out_row = 0;
	for (idx_t i = 0; i < input.size(); i++) {
		FetchAndParseRepo(input.data[0].GetValue(i).ToString(), bind.token, &opener, output, out_row++);
	}
	output.SetCardinality(out_row);
	return OperatorResultType::NEED_MORE_INPUT;
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
	return fn;
}

} // namespace duckdb
