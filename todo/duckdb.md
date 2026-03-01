# DuckDB upstream improvements

Changes to DuckDB core that would simplify or improve extensions that register
custom filesystems.  Each item describes the current limitation, why it matters
for `gh://`, and a proposed fix.

---

## 1 — `FileSystem::IsRemoteFile` should be delegatable to the VirtualFileSystem

**Location:** `duckdb/src/common/file_system.cpp` — `FileSystem::IsRemoteFile`

**Current behaviour:**
`IsRemoteFile` checks a hard-coded list (`EXTENSION_FILE_PREFIXES`).  Extensions
that register a custom `FileSystem` subclass (e.g. `GithubFileSystem` / `gh://`)
cannot declare themselves "remote" without patching DuckDB core.

**Why it matters — `ATTACH` auto-read-only promotion:**
`DatabaseManager::AttachDatabase` bumps `access_mode` to `READ_ONLY` for remote
paths (via `IsRemoteFile`).  Because `gh://` is unknown, plain
`ATTACH 'gh://…'` defaults to read-write and immediately fails when DuckDB tries
to open the database file for writing.  Users must spell out `(READ_ONLY)`
explicitly.

**Other callers affected:**
- `FileSystem::CanonicalizePath` — skips normalisation for remote paths (see item 2)
- `physical_copy_to_file`, extension install, home-directory checks

**Proposed fix:**
Add a virtual `IsRemoteFileSystem()` method to `FileSystem` (default `false`) and
teach `VirtualFileSystem::IsRemoteFile` to fall through to the registered subsystem:

```cpp
// FileSystem base class
virtual bool IsRemoteFileSystem() const { return false; }

// VirtualFileSystem
bool VirtualFileSystem::IsRemoteFile(const string &path, string &extension) {
    // existing EXTENSION_FILE_PREFIXES check …
    auto fs = FindFileSystemInternal(*registry, path);
    if (fs && fs->IsRemoteFileSystem()) {
        return true;
    }
    return false;
}
```

---

## 2 — `FileSystem::CanonicalizePath` collapses `://` double-slashes

**Location:** `duckdb/src/common/file_system.cpp` — `FileSystem::CanonicalizePath`

**Current behaviour:**
The base implementation splits the path on `/`, drops empty segments, and
rejoins.  This reduces `gh://owner/repo` to `gh:/owner/repo`, so
`CanHandleFile` never matches and the path falls through to `LocalFileSystem`.

**Workaround in this extension:**
`GithubFileSystem` overrides `CanonicalizePath` to return the path unchanged.

**Cleaner fix in DuckDB core:**
`CanonicalizePath` already delegates to `VirtualFileSystem::FindFileSystem` — if
`IsRemoteFile` (item 1) were fixed first, remote paths would be returned early
before the split/join happens, making the per-extension override unnecessary.
Alternatively, the split/join could be guarded to preserve `scheme://` prefixes
generically.

---

## 3 — `FileSystem::OpenFile` with `FILE_FLAGS_NULL_IF_NOT_EXISTS` is not documented for custom filesystems

**Location:** `duckdb/src/include/duckdb/common/file_open_flags.hpp`

**Current behaviour:**
Callers (e.g. WAL replay) open files with `FILE_FLAGS_NULL_IF_NOT_EXISTS` and
expect `nullptr` back when the file is absent.  This contract is not mentioned in
any documentation or header comment, so custom filesystem implementors easily miss
it and throw an exception instead of returning `nullptr`.

**Workaround in this extension:**
`GithubFileSystem::OpenFile` wraps `EnsureLoaded` in a `try/catch (IOException)`
when the flag is set.

**Suggested fix:**
Add a comment to `FileOpenFlags` and the `FileSystem::OpenFile` declaration
explaining the `NULL_IF_NOT_EXISTS` contract, and consider adding a helper or
assert in `VirtualFileSystem::OpenFileExtended` that catches misimplementations
in debug builds.
