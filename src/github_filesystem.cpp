#include "github_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "yyjson.hpp"

#include <cstring>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//===--------------------------------------------------------------------===//
// ParsedGHUrl
//===--------------------------------------------------------------------===//

ParsedGHUrl ParsedGHUrl::Parse(const string &url) {
	if (!StringUtil::StartsWith(url, "gh://")) {
		throw IOException("Invalid GitHub URL (must start with gh://): %s", url);
	}

	// Strip "gh://"
	string rest = url.substr(5);

	// Find owner
	auto slash1 = rest.find('/');
	if (slash1 == string::npos) {
		throw IOException("Invalid GitHub URL (missing repo): %s", url);
	}

	ParsedGHUrl result;
	result.owner = rest.substr(0, slash1);
	rest = rest.substr(slash1 + 1);

	// Find repo (possibly with @ref)
	auto slash2 = rest.find('/');
	string repo_ref;
	if (slash2 == string::npos) {
		repo_ref = rest;
		result.path = "";
	} else {
		repo_ref = rest.substr(0, slash2);
		result.path = rest.substr(slash2 + 1);
	}

	// Parse repo@ref
	auto at_pos = repo_ref.find('@');
	if (at_pos == string::npos) {
		result.repo = repo_ref;
		result.ref = ""; // will be resolved to default branch
	} else {
		result.repo = repo_ref.substr(0, at_pos);
		result.ref = repo_ref.substr(at_pos + 1);
	}

	if (result.owner.empty() || result.repo.empty()) {
		throw IOException("Invalid GitHub URL (empty owner or repo): %s", url);
	}

	return result;
}

string ParsedGHUrl::ToUrl() const {
	string url = "gh://" + owner + "/" + repo;
	if (!ref.empty()) {
		url += "@" + ref;
	}
	if (!path.empty()) {
		url += "/" + path;
	}
	return url;
}

//===--------------------------------------------------------------------===//
// GithubFileHandle
//===--------------------------------------------------------------------===//

GithubFileHandle::GithubFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags)
    : FileHandle(fs, path, flags) {
}

//===--------------------------------------------------------------------===//
// HTTP Helpers
//===--------------------------------------------------------------------===//

static HTTPUtil &GetHTTPUtil(optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	if (!db) {
		throw IOException("GitHub filesystem requires a database context");
	}
	return HTTPUtil::Get(*db);
}

static unique_ptr<HTTPResponse> MakeGetRequest(const string &url, const HTTPHeaders &extra_headers,
                                               optional_ptr<FileOpener> opener,
                                               std::function<bool(const_data_ptr_t, idx_t)> content_handler) {
	auto &http_util = GetHTTPUtil(opener);
	FileOpenerInfo info = {url};
	auto params = http_util.InitializeParameters(opener, &info);

	// Decompose URL into endpoint + path
	string path_out, proto_host_port;
	HTTPUtil::DecomposeURL(url, path_out, proto_host_port);

	// Build headers
	HTTPHeaders headers = extra_headers;

	GetRequestInfo request(proto_host_port, path_out, headers, *params,
	                       [](const HTTPResponse &) -> bool { return true; }, std::move(content_handler));

	auto client = http_util.InitializeClient(*params, proto_host_port);
	return client->Get(request);
}

//===--------------------------------------------------------------------===//
// GithubFileSystem helpers
//===--------------------------------------------------------------------===//

string GithubFileSystem::GetToken(optional_ptr<FileOpener> opener) {
	if (!opener) {
		return "";
	}
	auto sm = FileOpener::TryGetSecretManager(opener);
	if (!sm) {
		return "";
	}
	auto tx = FileOpener::TryGetCatalogTransaction(opener);
	if (!tx) {
		return "";
	}
	auto match = sm->LookupSecret(*tx, "gh://", "github");
	if (!match.HasMatch()) {
		return "";
	}
	auto &kv = static_cast<const KeyValueSecret &>(match.GetSecret());
	auto val = kv.TryGetValue("token");
	if (val.IsNull()) {
		return "";
	}
	return val.ToString();
}

string GithubFileSystem::CallAPI(const string &url, const string &token, optional_ptr<FileOpener> opener,
                                 bool *not_found) {
	HTTPHeaders headers;
	headers.Insert("Accept", "application/vnd.github+json");
	headers.Insert("X-GitHub-Api-Version", "2022-11-28");
	if (!token.empty()) {
		headers.Insert("Authorization", "Bearer " + token);
	}

	string body;
	auto response =
	    MakeGetRequest(url, headers, opener, [&body](const_data_ptr_t data, idx_t len) -> bool {
		    body.append(reinterpret_cast<const char *>(data), len);
		    return true;
	    });

	if (response->status == HTTPStatusCode::NotFound_404) {
		if (not_found) {
			*not_found = true;
		}
		return "";
	}
	if (!response->Success()) {
		throw IOException("GitHub API request to %s failed: %s", url, response->GetError());
	}
	return body;
}

string GithubFileSystem::FetchRaw(const string &url, optional_ptr<FileOpener> opener) {
	HTTPHeaders headers;
	string body;
	auto response =
	    MakeGetRequest(url, headers, opener, [&body](const_data_ptr_t data, idx_t len) -> bool {
		    body.append(reinterpret_cast<const char *>(data), len);
		    return true;
	    });

	if (!response->Success()) {
		throw IOException("Failed to fetch raw content from %s: %s", url, response->GetError());
	}
	return body;
}

string GithubFileSystem::ResolveDefaultBranch(const string &owner, const string &repo, const string &token,
                                              optional_ptr<FileOpener> opener) {
	string api_url = "https://api.github.com/repos/" + owner + "/" + repo;
	string body = CallAPI(api_url, token, opener);

	// Parse JSON: {"default_branch": "main", ...}
	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (!doc) {
		throw IOException("Failed to parse GitHub repo JSON for %s/%s", owner, repo);
	}
	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *branch_val = yyjson_obj_get(root, "default_branch");
	string branch;
	if (branch_val && yyjson_is_str(branch_val)) {
		branch = yyjson_get_str(branch_val);
	}
	yyjson_doc_free(doc);

	if (branch.empty()) {
		throw IOException("Could not determine default branch for %s/%s", owner, repo);
	}
	return branch;
}

void GithubFileSystem::EnsureLoaded(GithubFileHandle &handle, optional_ptr<FileOpener> opener) {
	if (handle.loaded) {
		return;
	}

	auto &pu = handle.parsed_url;
	string raw_url = "https://raw.githubusercontent.com/" + pu.owner + "/" + pu.repo + "/" + pu.ref + "/" + pu.path;
	string content = FetchRaw(raw_url, opener);

	handle.buffer.assign(content.begin(), content.end());
	handle.file_size = handle.buffer.size();
	handle.loaded = true;
}

//===--------------------------------------------------------------------===//
// GithubFileSystem – core methods
//===--------------------------------------------------------------------===//

bool GithubFileSystem::CanHandleFile(const string &fpath) {
	return StringUtil::StartsWith(fpath, "gh://");
}

unique_ptr<FileHandle> GithubFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	auto handle = make_uniq<GithubFileHandle>(*this, path, flags);
	handle->parsed_url = ParsedGHUrl::Parse(path);

	// Resolve default branch if not specified
	if (handle->parsed_url.ref.empty()) {
		string token = GetToken(opener);
		handle->parsed_url.ref = ResolveDefaultBranch(handle->parsed_url.owner, handle->parsed_url.repo, token, opener);
	}

	// Get file size from Contents API
	string token = GetToken(opener);
	auto &pu = handle->parsed_url;
	string api_url = "https://api.github.com/repos/" + pu.owner + "/" + pu.repo + "/contents/" + pu.path +
	                 "?ref=" + pu.ref;
	bool not_found = false;
	string body = CallAPI(api_url, token, opener, &not_found);

	if (not_found) {
		throw IOException("GitHub file not found: %s", path);
	}

	// Parse size from JSON
	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (doc) {
		yyjson_val *root = yyjson_doc_get_root(doc);
		yyjson_val *size_val = yyjson_obj_get(root, "size");
		if (size_val && yyjson_is_int(size_val)) {
			handle->file_size = static_cast<idx_t>(yyjson_get_int(size_val));
		}
		yyjson_doc_free(doc);
	}

	return std::move(handle);
}

void GithubFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &gh = handle.Cast<GithubFileHandle>();
	EnsureLoaded(gh, nullptr);

	if (location >= gh.file_size) {
		return;
	}
	idx_t available = gh.file_size - location;
	idx_t to_read = MinValue<idx_t>(static_cast<idx_t>(nr_bytes), available);
	memcpy(buffer, gh.buffer.data() + location, to_read);
}

int64_t GithubFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gh = handle.Cast<GithubFileHandle>();
	EnsureLoaded(gh, nullptr);

	if (gh.file_offset >= gh.file_size) {
		return 0;
	}
	idx_t available = gh.file_size - gh.file_offset;
	idx_t to_read = MinValue<idx_t>(static_cast<idx_t>(nr_bytes), available);
	memcpy(buffer, gh.buffer.data() + gh.file_offset, to_read);
	gh.file_offset += to_read;
	return static_cast<int64_t>(to_read);
}

int64_t GithubFileSystem::GetFileSize(FileHandle &handle) {
	auto &gh = handle.Cast<GithubFileHandle>();
	return static_cast<int64_t>(gh.file_size);
}

void GithubFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<GithubFileHandle>().file_offset = location;
}

idx_t GithubFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<GithubFileHandle>().file_offset;
}

bool GithubFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	if (!CanHandleFile(filename)) {
		return false;
	}
	try {
		auto pu = ParsedGHUrl::Parse(filename);
		string token = GetToken(opener);
		if (pu.ref.empty()) {
			pu.ref = ResolveDefaultBranch(pu.owner, pu.repo, token, opener);
		}
		string api_url = "https://api.github.com/repos/" + pu.owner + "/" + pu.repo + "/contents/" + pu.path +
		                 "?ref=" + pu.ref;
		bool not_found = false;
		CallAPI(api_url, token, opener, &not_found);
		return !not_found;
	} catch (...) {
		return false;
	}
}

bool GithubFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                  FileOpener *opener) {
	if (!CanHandleFile(directory)) {
		return false;
	}
	try {
		auto pu = ParsedGHUrl::Parse(directory);
		optional_ptr<FileOpener> opt_opener(opener);
		string token = GetToken(opt_opener);
		if (pu.ref.empty()) {
			pu.ref = ResolveDefaultBranch(pu.owner, pu.repo, token, opt_opener);
		}

		string api_url = "https://api.github.com/repos/" + pu.owner + "/" + pu.repo + "/contents/" + pu.path +
		                 "?ref=" + pu.ref;
		bool not_found = false;
		string body = CallAPI(api_url, token, opt_opener, &not_found);
		if (not_found || body.empty()) {
			return false;
		}

		yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
		if (!doc) {
			return false;
		}
		yyjson_val *arr = yyjson_doc_get_root(doc);
		if (!yyjson_is_arr(arr)) {
			yyjson_doc_free(doc);
			return false;
		}

		yyjson_val *item;
		size_t idx, max;
		yyjson_arr_foreach(arr, idx, max, item) {
			yyjson_val *type_val = yyjson_obj_get(item, "type");
			yyjson_val *name_val = yyjson_obj_get(item, "name");
			if (!type_val || !name_val) {
				continue;
			}
			const char *type_str = yyjson_get_str(type_val);
			const char *name_str = yyjson_get_str(name_val);
			if (!type_str || !name_str) {
				continue;
			}
			bool is_dir = (strcmp(type_str, "dir") == 0);
			callback(name_str, is_dir);
		}
		yyjson_doc_free(doc);
		return true;
	} catch (...) {
		return false;
	}
}

void GithubFileSystem::GlobRecursive(const string &owner, const string &repo, const string &ref, const string &dir,
                                      const string &full_pattern, vector<OpenFileInfo> &results, const string &token,
                                      optional_ptr<FileOpener> opener) {
	string api_url = "https://api.github.com/repos/" + owner + "/" + repo + "/contents/" + dir + "?ref=" + ref;
	bool not_found = false;
	string body = CallAPI(api_url, token, opener, &not_found);
	if (not_found || body.empty()) {
		return;
	}

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (!doc) {
		return;
	}
	yyjson_val *arr = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(arr)) {
		yyjson_doc_free(doc);
		return;
	}

	yyjson_val *item;
	size_t idx, max;
	yyjson_arr_foreach(arr, idx, max, item) {
		yyjson_val *type_val = yyjson_obj_get(item, "type");
		yyjson_val *path_val = yyjson_obj_get(item, "path");
		if (!type_val || !path_val) {
			continue;
		}
		const char *type_str = yyjson_get_str(type_val);
		const char *file_path = yyjson_get_str(path_val);
		if (!type_str || !file_path) {
			continue;
		}

		string full_url = "gh://" + owner + "/" + repo + "@" + ref + "/" + file_path;

		if (strcmp(type_str, "dir") == 0) {
			GlobRecursive(owner, repo, ref, file_path, full_pattern, results, token, opener);
		} else if (strcmp(type_str, "file") == 0) {
			// Check if the full URL matches the pattern
			if (::duckdb::Glob(full_url.c_str(), full_url.size(), full_pattern.c_str(), full_pattern.size())) {
				results.emplace_back(full_url);
			}
		}
	}
	yyjson_doc_free(doc);
}

vector<OpenFileInfo> GithubFileSystem::Glob(const string &path, FileOpener *opener) {
	if (!CanHandleFile(path)) {
		return {};
	}

	// If no wildcard, just check existence
	if (!FileSystem::HasGlob(path)) {
		optional_ptr<FileOpener> opt_opener(opener);
		if (FileExists(path, opt_opener)) {
			return {OpenFileInfo(path)};
		}
		return {};
	}

	try {
		auto pu = ParsedGHUrl::Parse(path);
		optional_ptr<FileOpener> opt_opener(opener);
		string token = GetToken(opt_opener);
		if (pu.ref.empty()) {
			pu.ref = ResolveDefaultBranch(pu.owner, pu.repo, token, opt_opener);
		}

		// Build canonical pattern with resolved ref
		string full_pattern = "gh://" + pu.owner + "/" + pu.repo + "@" + pu.ref + "/" + pu.path;

		// Find the base directory (part before first wildcard in path)
		string base_dir;
		auto star_pos = pu.path.find_first_of("*?[");
		if (star_pos != string::npos) {
			auto last_slash = pu.path.rfind('/', star_pos);
			if (last_slash != string::npos) {
				base_dir = pu.path.substr(0, last_slash);
			}
			// if no slash before first wildcard, base_dir = "" (root of repo)
		} else {
			base_dir = pu.path;
		}

		vector<OpenFileInfo> results;
		GlobRecursive(pu.owner, pu.repo, pu.ref, base_dir, full_pattern, results, token, opt_opener);
		return results;
	} catch (...) {
		return {};
	}
}

} // namespace duckdb
