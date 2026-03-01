#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"

namespace duckdb {

struct ParsedGHUrl {
	string owner; // "duckdb"
	string repo;  // "duckdb"
	string ref;   // "main" (empty = resolve default branch)
	string path;  // "data/csv/test.csv" (no leading slash)

	static ParsedGHUrl Parse(const string &url);
	string ToUrl() const;
};

struct GithubFileHandle : public FileHandle {
public:
	GithubFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags);
	void Close() override {
	}

	ParsedGHUrl parsed_url;
	idx_t file_size = 0;
	idx_t file_offset = 0;
	vector<char> buffer;
	bool loaded = false;
};

class GithubFileSystem : public FileSystem {
public:
	bool CanHandleFile(const string &fpath) override;
	string GetName() const override {
		return "GithubFileSystem";
	}
	bool CanSeek() override {
		return true;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	bool SupportsGlobExtended() const override {
		return true;
	}
	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener = nullptr) override;

	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return timestamp_t(0);
	}

	// Public so GithubGlobResult can call them
	static string GetToken(optional_ptr<FileOpener> opener);
	static string CallAPI(const string &url, const string &token, optional_ptr<FileOpener> opener,
	                      bool *not_found = nullptr);

private:
	static string ResolveDefaultBranch(const string &owner, const string &repo, const string &token,
	                                   optional_ptr<FileOpener> opener);
	static void EnsureLoaded(GithubFileHandle &handle, optional_ptr<FileOpener> opener);
};

} // namespace duckdb
