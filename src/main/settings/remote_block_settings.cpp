#include "duckdb/main/settings.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
namespace duckdb {

void UseRemoteBlockManagerSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
  config.options.use_remote_block_manager = input.GetValue<bool>();
}

void UseRemoteBlockManagerSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
  config.options.use_remote_block_manager = false;
}

Value UseRemoteBlockManagerSetting::GetSetting(const ClientContext &context) {
  return Value::BOOLEAN(DBConfig::GetConfig(context).options.use_remote_block_manager);
}

}
