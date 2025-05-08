//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_access_tracker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/storage_info.hpp"
#include <fstream>
#include <atomic>
namespace duckdb {

enum class BlockAccessType : uint8_t {
    READ = 1,
    WRITE = 2,
    REMOVE = 3,
    LOAD = 4
};

//! The BlockAccessTracker is responsible for tracking all block accesses (reads/writes)
//! and writing them to a trace file for analysis
class BlockAccessTracker {
public:
    //! Constructor
    explicit BlockAccessTracker(const string &trace_path);
    //! Destructor
    ~BlockAccessTracker();

    //! Record a block access
    void RecordAccess(block_id_t block_id, BlockAccessType access_type, uint64_t block_size, const string &source_info = "");

    //! Enable or disable tracking
    void SetEnabled(bool enabled);

    //! Check if tracking is enabled
    bool IsEnabled() const;

    //! Flush the trace file
    void Flush();

private:
    //! The trace file
    std::ofstream trace_file;
    //! Whether tracking is enabled
    std::atomic<bool> enabled;
    //! Mutex for thread-safe access
    mutex tracker_lock;
};

} // namespace duckdb