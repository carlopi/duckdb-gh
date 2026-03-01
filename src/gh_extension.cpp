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
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

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

	// Register table functions
	loader.RegisterFunction(GithubRepoFunction());

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
