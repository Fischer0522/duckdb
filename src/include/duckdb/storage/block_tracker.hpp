//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_tracker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

enum class BlockAccessType : uint8_t {
    READ = 1,
    WRITE = 2,
    ERASE = 3,
};

//! BlockTracker tracks block read/write operations and logs them to a CSV file
class BlockTracker {
public:
    BlockTracker(DatabaseInstance &db, const string &csv_path);
    ~BlockTracker();

    //! Track a block read operation
    void TrackRead(block_id_t block_id, idx_t block_size, const string &source_info);
    
    //! Track a block write operation
    void TrackWrite(block_id_t block_id, idx_t block_size, const string &source_info);

    //! Track a block erase operation
    void TrackErase(block_id_t block_id, idx_t block_size, const string &source_info);

    //! Get the singleton instance for the given database
    static BlockTracker &GetInstance(DatabaseInstance &db);
    
    //! Get the singleton instance for the given client context
    static BlockTracker &GetInstance(ClientContext &context);

private:
    //! Internal method to track block access
    void TrackAccess(block_id_t block_id, BlockAccessType access_type, idx_t block_size, const string &source_info);
    
    //! Initialize the CSV file with headers
    void InitializeCSV();

private:
    //! The database instance
    DatabaseInstance &db;
    
    //! Path to the CSV file
    string csv_path;
    
    //! File handle for the CSV
    unique_ptr<FileHandle> file_handle;
    
    //! Mutex for thread-safe access
    mutex lock;
    
    //! Whether the CSV has been initialized
    bool initialized;
    
    //! Map of database instances to their trackers
    static unordered_map<DatabaseInstance *, unique_ptr<BlockTracker>> instance_map;
    
    //! Mutex for thread-safe access to the instance map
    static mutex instance_map_lock;
};

} // namespace duckdb