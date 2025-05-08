#include "duckdb/storage/block_access_tracker.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include <chrono>

namespace duckdb {

BlockAccessTracker::BlockAccessTracker(const string &trace_path) : enabled(false) {
    if (trace_path.empty()) {
        return;
    }
    
    try {
        trace_file.open(trace_path, std::ios::out | std::ios::trunc);
        if (trace_file.is_open()) {
            // Write header
            trace_file << "timestamp,block_id,access_type,block_size,source_info" << std::endl;
            enabled = true;
        }
    } catch (std::exception &e) {
        // Silently fail - tracing is optional
    }
}

BlockAccessTracker::~BlockAccessTracker() {
    if (trace_file.is_open()) {
        trace_file.close();
    }
}

void BlockAccessTracker::RecordAccess(block_id_t block_id, BlockAccessType access_type, uint64_t block_size, const string &source_info) {
    if (!enabled) {
        return;
    }

    lock_guard<mutex> lock(tracker_lock);
    if (!trace_file.is_open()) {
        return;
    }

    // Get current timestamp
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    

    // Write access record
    trace_file << timestamp << ","
               << block_id << ","
               << static_cast<int>(access_type) << ","
               << block_size << ","
               << source_info << std::endl;
}

void BlockAccessTracker::SetEnabled(bool enabled_param) {
    enabled = enabled_param;
}

bool BlockAccessTracker::IsEnabled() const {
    return enabled;
}

void BlockAccessTracker::Flush() {
    if (!enabled) {
        return;
    }

    lock_guard<mutex> lock(tracker_lock);
    if (trace_file.is_open()) {
        trace_file.flush();
    }
}

} // namespace duckdb