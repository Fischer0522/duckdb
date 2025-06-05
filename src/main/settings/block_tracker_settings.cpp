#include "duckdb/main/settings.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/block_tracker.hpp"

namespace duckdb {

void TrackBlockAccessSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter) {
    auto new_value = parameter.GetValue<bool>();
    config.options.track_block_access = new_value;
}

void TrackBlockAccessSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
    config.options.track_block_access = false;
}

Value TrackBlockAccessSetting::GetSetting(const ClientContext &context) {
    return Value::BOOLEAN(DBConfig::GetConfig(context).options.track_block_access);
}

void BlockAccessTrackingFileSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter) {
    auto new_value = parameter.GetValue<string>();
    config.options.block_access_tracking_file = new_value;
}

void BlockAccessTrackingFileSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
    config.options.block_access_tracking_file = "block_trace.csv";
}

Value BlockAccessTrackingFileSetting::GetSetting(const ClientContext &context) {
    return Value(DBConfig::GetConfig(context).options.block_access_tracking_file);
}

} // namespace duckdb