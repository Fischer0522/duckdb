#include "duckdb/storage/block_tracker.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

unordered_map<DatabaseInstance *, unique_ptr<BlockTracker>> BlockTracker::instance_map;
mutex BlockTracker::instance_map_lock;

BlockTracker::BlockTracker(DatabaseInstance &db, const string &csv_path)
    : db(db), csv_path(csv_path), initialized(false) {
    InitializeCSV();
}

BlockTracker::~BlockTracker() {
    if (file_handle) {
        file_handle->Close();
    }
}

void BlockTracker::InitializeCSV() {
    lock_guard<mutex> guard(lock);
    if (initialized) {
        return;
    }

    auto &fs = FileSystem::GetFileSystem(db);
    file_handle = fs.OpenFile(csv_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
    
    // Write CSV header
    string header = "timestamp       , block_id           , access_type, block_size, source_info\n";
    file_handle->Write((void *)header.c_str(), header.size());
    file_handle->Sync();
    
    initialized = true;
}

void BlockTracker::TrackAccess(block_id_t block_id, BlockAccessType access_type, idx_t block_size, const string &source_info) {
    lock_guard<mutex> guard(lock);
    if (!initialized || !file_handle) {
        return;
    }

    // Get current timestamp in microseconds
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    
    // Format the CSV line
    string line = StringUtil::Format("%lld, %lld, %d, %lld, %s\n", 
                                    timestamp, 
                                    (int64_t)block_id, 
                                    (int)access_type, 
                                    (int64_t)block_size, 
                                    source_info);
    
    // Write to file
    file_handle->Write((void *)line.c_str(), line.size());
    // We don't sync after every write for performance reasons
}

void BlockTracker::TrackRead(block_id_t block_id, idx_t block_size, const string &source_info) {
    TrackAccess(block_id, BlockAccessType::READ, block_size, source_info);
}

void BlockTracker::TrackWrite(block_id_t block_id, idx_t block_size, const string &source_info) {
    TrackAccess(block_id, BlockAccessType::WRITE, block_size, source_info);
}

void BlockTracker::TrackErase(block_id_t block_id, idx_t block_size, const string &source_info) {
    TrackAccess(block_id, BlockAccessType::ERASE, block_size, source_info);
}

BlockTracker &BlockTracker::GetInstance(DatabaseInstance &db) {
    lock_guard<mutex> guard(instance_map_lock);
    auto it = instance_map.find(&db);
    if (it == instance_map.end()) {
        auto &config = DBConfig::GetConfig(db);
        if (!config.options.track_block_access) {
            throw InternalException("Block tracking is not enabled in configuration");
        }
        auto tracker = make_uniq<BlockTracker>(db, config.options.block_access_tracking_file);
        auto result = tracker.get();
        instance_map[&db] = std::move(tracker);
        return *result;
    }
    return *it->second;
}

BlockTracker &BlockTracker::GetInstance(ClientContext &context) {
    return GetInstance(DatabaseInstance::GetDatabase(context));
}

} // namespace duckdb