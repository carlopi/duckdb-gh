#include "github_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "yyjson.hpp"

#include <cstdlib>
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

static unique_ptr<HTTPResponse> MakeGetRequest(const string &url, const HTTPHeaders &extra_headers,
                                               optional_ptr<FileOpener> opener,
                                               std::function<bool(const_data_ptr_t, idx_t)> content_handler) {
	auto db = FileOpener::TryGetDatabase(opener);
	if (!db) {
		throw IOException("GitHub filesystem requires a database context");
	}
	auto &http_util = HTTPUtil::Get(*db);
	FileOpenerInfo info = {url};
	auto params = http_util.InitializeParameters(opener, &info);

        auto client_context = FileOpener::TryGetClientContext(opener);
        if (client_context) {
                auto &client_config = ClientConfig::GetConfig(*client_context);
                if (client_config.enable_http_logging) {
                        params->logger = client_context->logger;
                }
        }


	// Decompose URL into endpoint + path
	string path_out, proto_host_port;
	HTTPUtil::DecomposeURL(url, path_out, proto_host_port);

	HTTPHeaders headers = extra_headers;
		std::cout << proto_host_port << "\t" << path_out << "\n";
	GetRequestInfo request(proto_host_port + path_out, extra_headers, *params,
	                       [](const HTTPResponse &) -> bool { return true; }, std::move(content_handler));

	auto client = http_util.InitializeClient(*params, proto_host_port);
	return client->Get(request);
}

//===--------------------------------------------------------------------===//
// GithubFileSystem helpers
//===--------------------------------------------------------------------===//

string GithubFileSystem::GetToken(optional_ptr<FileOpener> opener) {
	// 1. Try a DuckDB secret (CREATE SECRET ... TYPE github ...)
	if (opener) {
		auto sm = FileOpener::TryGetSecretManager(opener);
		auto tx = FileOpener::TryGetCatalogTransaction(opener);
		if (sm && tx) {
			auto match = sm->LookupSecret(*tx, "gh://", "github");
			if (match.HasMatch()) {
				auto &kv = static_cast<const KeyValueSecret &>(match.GetSecret());
				auto val = kv.TryGetValue("token");
				if (!val.IsNull()) {
					return val.ToString();
				}
			}
		}
	}
	// 2. Fall back to GITHUB_TOKEN environment variable
	const char *env_token = std::getenv("GITHUB_TOKEN");
	if (env_token && env_token[0] != '\0') {
		return string(env_token);
	}
	return "";
}

string GithubFileSystem::CallAPI(const string &url, const string &token, optional_ptr<FileOpener> opener,
                                 bool *not_found) {
	HTTPHeaders headers;
	headers.Insert("Accept", "application/vnd.github+json");
	headers.Insert("X-GitHub-Api-Version", "2022-11-28");
headers.Insert("User-Agent", "HOHOHO");
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
	// opener must be non-null: EnsureLoaded should only be called from OpenFile
	// where the opener is always available.
	if (!opener) {
		throw IOException("GithubFileSystem: cannot load '%s' without a file opener", handle.path);
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

	// When no ref is given, use "HEAD" — raw.githubusercontent.com resolves it to the default
	// branch automatically, so no extra API call is needed.
	if (handle->parsed_url.ref.empty()) {
		handle->parsed_url.ref = "HEAD";
	}

	// Eagerly download content now while we have access to the opener.
	// Read() has no opener parameter in the FileSystem interface, so we must
	// fetch here rather than deferring to the first Read() call.
	EnsureLoaded(*handle, opener);

	return std::move(handle);
}

void GithubFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &gh = handle.Cast<GithubFileHandle>();

	if (location >= gh.file_size) {
		return;
	}
	idx_t available = gh.file_size - location;
	idx_t to_read = MinValue<idx_t>(static_cast<idx_t>(nr_bytes), available);
	memcpy(buffer, gh.buffer.data() + location, to_read);
}

int64_t GithubFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gh = handle.Cast<GithubFileHandle>();

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
		// Use "HEAD" when no branch is given — raw.githubusercontent.com resolves it automatically.
		const string &effective_ref = pu.ref.empty() ? "HEAD" : pu.ref;
		// Check existence via raw.githubusercontent.com (avoids GitHub Contents API 403 / rate limits).
		string raw_url = "https://raw.githubusercontent.com/" + pu.owner + "/" + pu.repo + "/" + effective_ref +
		                 "/" + pu.path;
		// Discard body — we only need the HTTP status code.
		auto response = MakeGetRequest(raw_url, HTTPHeaders {}, opener,
		                               [](const_data_ptr_t, idx_t) -> bool { return true; });
		return response && response->status != HTTPStatusCode::NotFound_404 && response->Success();
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
			pu.ref = "HEAD";
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

//===--------------------------------------------------------------------===//
// GithubGlobResult – LazyMultiFileList using Contents API
//===--------------------------------------------------------------------===//

// Segment-by-segment glob match. When completed=false the key is treated as a
// prefix: if all key segments are consumed before the pattern is exhausted the
// function returns true, meaning the directory *could* still lead to a match.
// Used to prune subdirectories that can never contribute a matching file.
static bool SegmentMatch(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                         vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end,
                         bool completed) {
	if (key == key_end && !completed) {
		return true;
	}
	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			pattern++;
			while (key != key_end) {
				if (SegmentMatch(key, key_end, pattern, pattern_end, completed)) {
					return true;
				}
				key++;
			}
			return !completed;
		}
		if (!::duckdb::Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	if (pattern != pattern_end && !completed) {
		return true;
	}
	return key == key_end && pattern == pattern_end;
}

// Each ExpandNextPath() call processes one directory from the queue using the
// Contents API, expanding files that match the glob and enqueuing subdirs.
struct GithubGlobResult : public LazyMultiFileList {
public:
	GithubGlobResult(GithubFileSystem &fs, const string &glob_pattern, optional_ptr<FileOpener> opener);

protected:
	bool ExpandNextPath() const override;

private:
	GithubFileSystem &fs;
	optional_ptr<FileOpener> opener;

	// Resolved at construction
	ParsedGHUrl parsed_url;
	string full_pattern;         // canonical pattern with ref for glob matching
	string token;
	vector<string> pattern_splits; // parsed_url.path split by '/' for SegmentMatch

	mutable vector<string> pending_dirs; // BFS queue of repo-relative dir paths
};

GithubGlobResult::GithubGlobResult(GithubFileSystem &fs_p, const string &glob_pattern,
                                   optional_ptr<FileOpener> opener_p)
    : LazyMultiFileList(FileOpener::TryGetClientContext(opener_p)), fs(fs_p), opener(opener_p) {

	if (!FileSystem::HasGlob(glob_pattern)) {
		if (fs.FileExists(glob_pattern, opener)) {
			expanded_files.emplace_back(glob_pattern);
		}
		return; // all_files_expanded will be set when ExpandNextPath returns false
	}

	parsed_url = ParsedGHUrl::Parse(glob_pattern);
	if (parsed_url.ref.empty()) {
		parsed_url.ref = "HEAD";
	}

	full_pattern = "gh://" + parsed_url.owner + "/" + parsed_url.repo + "@" + parsed_url.ref + "/" + parsed_url.path;
	token = GithubFileSystem::GetToken(opener);
	pattern_splits = StringUtil::Split(parsed_url.path, "/");

	// Seed the BFS queue with the base dir (path before first wildcard)
	auto star_pos = parsed_url.path.find_first_of("*?[");
	string base_dir;
	if (star_pos != string::npos) {
		auto last_slash = parsed_url.path.rfind('/', star_pos);
		base_dir = (last_slash != string::npos) ? parsed_url.path.substr(0, last_slash) : "";
	} else {
		base_dir = parsed_url.path;
	}
	pending_dirs.push_back(base_dir);
}

bool GithubGlobResult::ExpandNextPath() const {
	if (pending_dirs.empty()) {
		return false;
	}

	string dir = pending_dirs.back();
	pending_dirs.pop_back();

	string api_url = "https://api.github.com/repos/" + parsed_url.owner + "/" + parsed_url.repo + "/contents/" + dir +
	                 "?ref=" + parsed_url.ref;
	bool not_found = false;
	string body = fs.CallAPI(api_url, token, opener, &not_found);
	if (not_found || body.empty()) {
		return !pending_dirs.empty();
	}

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (!doc) {
		return !pending_dirs.empty();
	}
	yyjson_val *arr = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(arr)) {
		yyjson_doc_free(doc);
		return !pending_dirs.empty();
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
		const char *entry_path = yyjson_get_str(path_val);
		if (!type_str || !entry_path) {
			continue;
		}
		if (strcmp(type_str, "dir") == 0) {
			// Only recurse if the directory path can still lead to a match
			auto key_splits = StringUtil::Split(entry_path, "/");
			if (SegmentMatch(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end(),
			                 false)) {
				pending_dirs.push_back(entry_path);
			}
		} else if (strcmp(type_str, "file") == 0) {
			string full_url =
			    "gh://" + parsed_url.owner + "/" + parsed_url.repo + "@" + parsed_url.ref + "/" + entry_path;
			if (::duckdb::Glob(full_url.c_str(), full_url.size(), full_pattern.c_str(), full_pattern.size())) {
				expanded_files.emplace_back(full_url);
			}
		}
	}
	yyjson_doc_free(doc);
	return !pending_dirs.empty();
}

//===--------------------------------------------------------------------===//
// GithubTreesGlobResult – LazyMultiFileList using Git Trees API
//===--------------------------------------------------------------------===//

// Used for patterns containing '**'. A single API call fetches the full
// recursive tree; results are filtered client-side. No directory-level pruning
// is possible once '**' is in the pattern, so the round-trip cost dominates.
struct GithubTreesGlobResult : public LazyMultiFileList {
public:
	GithubTreesGlobResult(GithubFileSystem &fs, const string &glob_pattern, optional_ptr<FileOpener> opener);

protected:
	bool ExpandNextPath() const override;

private:
	GithubFileSystem &fs;
	optional_ptr<FileOpener> opener;
	mutable bool finished = false;

	ParsedGHUrl parsed_url;
	string full_pattern; // canonical pattern with ref for glob matching
	string token;
	string path_prefix; // base_dir + "/" used for cheap pre-filter
};

GithubTreesGlobResult::GithubTreesGlobResult(GithubFileSystem &fs_p, const string &glob_pattern,
                                             optional_ptr<FileOpener> opener_p)
    : LazyMultiFileList(FileOpener::TryGetClientContext(opener_p)), fs(fs_p), opener(opener_p) {

	parsed_url = ParsedGHUrl::Parse(glob_pattern);
	if (parsed_url.ref.empty()) {
		parsed_url.ref = "HEAD";
	}

	full_pattern = "gh://" + parsed_url.owner + "/" + parsed_url.repo + "@" + parsed_url.ref + "/" + parsed_url.path;
	token = GithubFileSystem::GetToken(opener);

	// Derive the longest literal prefix before the first wildcard for cheap filtering
	auto star_pos = parsed_url.path.find_first_of("*?[");
	string base_dir;
	if (star_pos != string::npos) {
		auto last_slash = parsed_url.path.rfind('/', star_pos);
		base_dir = (last_slash != string::npos) ? parsed_url.path.substr(0, last_slash) : "";
	} else {
		base_dir = parsed_url.path;
	}
	path_prefix = base_dir.empty() ? "" : (base_dir + "/");
}

bool GithubTreesGlobResult::ExpandNextPath() const {
	if (finished) {
		return false;
	}
	finished = true;

	// Single call: GET /repos/{owner}/{repo}/git/trees/{ref}?recursive=1
	string api_url = "https://api.github.com/repos/" + parsed_url.owner + "/" + parsed_url.repo + "/git/trees/" +
	                 parsed_url.ref + "?recursive=1";

	bool not_found = false;
	string body = fs.CallAPI(api_url, token, opener, &not_found);
	if (not_found || body.empty()) {
		return false;
	}

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
	if (!doc) {
		return false;
	}
	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *tree_arr = yyjson_obj_get(root, "tree");
	if (!tree_arr || !yyjson_is_arr(tree_arr)) {
		yyjson_doc_free(doc);
		return false;
	}

	yyjson_val *item;
	size_t idx, max;
	yyjson_arr_foreach(tree_arr, idx, max, item) {
		yyjson_val *type_val = yyjson_obj_get(item, "type");
		yyjson_val *path_val = yyjson_obj_get(item, "path");
		if (!type_val || !path_val) {
			continue;
		}
		const char *type_str = yyjson_get_str(type_val);
		const char *entry_path = yyjson_get_str(path_val);
		if (!type_str || !entry_path || strcmp(type_str, "blob") != 0) {
			continue;
		}
		if (!path_prefix.empty() && !StringUtil::StartsWith(entry_path, path_prefix)) {
			continue;
		}
		string full_url =
		    "gh://" + parsed_url.owner + "/" + parsed_url.repo + "@" + parsed_url.ref + "/" + entry_path;
		if (::duckdb::Glob(full_url.c_str(), full_url.size(), full_pattern.c_str(), full_pattern.size())) {
			expanded_files.emplace_back(full_url);
		}
	}
	yyjson_doc_free(doc);
	return false;
}

//===--------------------------------------------------------------------===//
// GlobFilesExtended – dispatch to Trees or BFS strategy
//===--------------------------------------------------------------------===//

unique_ptr<MultiFileList> GithubFileSystem::GlobFilesExtended(const string &path, const FileGlobInput &input,
                                                               optional_ptr<FileOpener> opener) {
	// '**' means unlimited depth — no directory pruning is possible, so pay
	// one API call for the full recursive tree and filter client-side.
	// Single-level wildcards ('*', '?', '[') keep enough structure for the
	// BFS + SegmentMatch pruning to skip irrelevant subtrees.
	if (StringUtil::Contains(path, "**")) {
		return make_uniq<GithubTreesGlobResult>(*this, path, opener);
	}
	return make_uniq<GithubGlobResult>(*this, path, opener);
}

} // namespace duckdb
