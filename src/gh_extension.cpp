#define DUCKDB_EXTENSION_MAIN

#include "gh_extension.hpp"
#include "github_filesystem.hpp"
#include "github_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register GitHub secret type
	SecretType github_secret_type;
	github_secret_type.name = "github";
	github_secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	github_secret_type.default_provider = "config";
	loader.RegisterSecretType(github_secret_type);

	// Register GitHub secret provider (config)
	CreateSecretFunction github_secret_fn;
	github_secret_fn.secret_type = "github";
	github_secret_fn.provider = "config";
	github_secret_fn.named_parameters["token"] = LogicalType::VARCHAR;
	github_secret_fn.function = [](ClientContext &, CreateSecretInput &input) -> unique_ptr<BaseSecret> {
		auto scope = input.scope.empty() ? vector<string> {"gh://"} : input.scope;
		auto s = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
		if (input.options.count("token")) {
			s->secret_map["token"] = input.options.at("token");
		}
		s->redact_keys.insert("token");
		return unique_ptr<BaseSecret>(std::move(s));
	};
	loader.RegisterFunction(github_secret_fn);

	// Register gh_repo('owner/repo') with inline description and examples
	{
		CreateTableFunctionInfo info(GithubRepoFunction());
		FunctionDescription desc;
		desc.parameter_names = {"repo"};
		desc.parameter_types = {LogicalType::VARCHAR};
		desc.description = "Fetches GitHub repository metadata for a single repository or all repositories "
		                   "for an org/user. Pass 'owner/repo' for one repo, or 'owner/*' to expand to "
		                   "all repos belonging to that org or user.";
		desc.examples = {
		    "SELECT name, stargazers_count, language FROM gh_repo('duckdb/duckdb');",
		    "SELECT name, stargazers_count FROM gh_repo('my-org/*') ORDER BY stargazers_count DESC;",
		};
		desc.categories = {"github"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(std::move(info));
	}

	// Register gh_repos((table)) with inline description and examples
	{
		CreateTableFunctionInfo info(GithubReposFunction());
		FunctionDescription desc;
		desc.parameter_names = {"repos"};
		desc.parameter_types = {LogicalType::TABLE};
		desc.description = "Table in-out function that fetches GitHub repository metadata for each "
		                   "'owner/repo' string in the input table. 'owner/*' rows are expanded to all "
		                   "repositories for that org or user. Accepts any subquery or VALUES list that "
		                   "returns a single VARCHAR column.";
		desc.examples = {
		    "SELECT name, stargazers_count FROM gh_repos((VALUES ('duckdb/duckdb'), ('duckdb/pg_duckdb')));",
		    "SELECT r.name, r.language FROM my_repos, gh_repos((SELECT repo_name FROM my_repos)) r;",
		};
		desc.categories = {"github"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(std::move(info));
	}

	// Register gh_issues('owner/repo') with inline description and examples
	{
		CreateTableFunctionInfo info(GithubIssuesFunction());
		FunctionDescription desc;
		desc.parameter_names = {"repo"};
		desc.parameter_types = {LogicalType::VARCHAR};
		desc.description = "Fetches GitHub issues for a repository as a table, one row per issue. "
		                   "Pull requests are excluded. Paginates automatically. "
		                   "The optional 'state' parameter filters by issue state: "
		                   "'open' (default), 'closed', or 'all'.";
		desc.examples = {
		    "SELECT number, title, user FROM gh_issues('duckdb/duckdb');",
		    "SELECT number, title FROM gh_issues('duckdb/duckdb', state := 'closed') ORDER BY closed_at DESC;",
		    "SELECT count(*) FROM gh_issues('duckdb/duckdb', state := 'all');",
		};
		desc.categories = {"github"};
		info.descriptions.push_back(std::move(desc));
		loader.RegisterFunction(std::move(info));
	}

	// Register GitHub filesystem
	auto &db = loader.GetDatabaseInstance();
	auto &db_fs = db.GetFileSystem();
	db_fs.RegisterSubSystem(make_uniq<GithubFileSystem>());

	// httpfs provides the HTTPS backend needed for gh:// requests.
	// Try to load it if it is already installed; fail silently if not.
	ExtensionHelper::TryAutoLoadExtension(db, "httpfs");
	ExtensionHelper::TryAutoLoadExtension(db, "json");
}

void GhExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string GhExtension::Name() {
	return "gh";
}

std::string GhExtension::Version() const {
#ifdef EXT_VERSION_GH
	return EXT_VERSION_GH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(gh, loader) {
	duckdb::LoadInternal(loader);
}
}
