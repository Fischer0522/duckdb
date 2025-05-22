#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include "config.h"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/helper.hpp"
#include "catch.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/remote_block_manager.hpp"
#include "duckdb/common/stacktrace.hpp"


using namespace duckdb;
std::unique_ptr<mpool::Config> init_config() {
  auto config = make_uniq<mpool::Config>();
  config->hn_host = "127.0.0.1";
  config->hn_user = "root";
  config->hn_password = "123123";
  config->hn_port = 2250;
  config->pmd_id = 1;
  config->remote_memory_size = 2147483648;
  config->local_memory_size = static_cast<uint64_t>(1 * 128 * 1024 * 1024);
  config->alloc_sizes = {512, 1024, 4096, 8192};
  config->eviction_strategy = "lru";
  config->background_evcition = false;
  return std::move(config);
}

TEST_CASE("Test read write", "[mem_pool]") {
  DuckDB db(nullptr);
  auto ins = db.instance;
  RemoteBlockManager* block_manager = new RemoteBlockManager(*ins);
  std::cout << std::endl << "init TempBlockmanager success" << std::endl;
  auto block_size = DEFAULT_BLOCK_ALLOC_SIZE - Storage::DEFAULT_BLOCK_HEADER_SIZE;
  auto buf = ins->GetBufferManager().ConstructManagedBuffer(block_size, nullptr, FileBufferType::MANAGED_BUFFER);
  // fill this buffer
  auto internal = buf->InternalBuffer(); 
  memset(internal, 1, block_size);
  D_ASSERT(buf != nullptr);
  block_manager->WriteTemporaryBuffer(1,*buf);
  std::cout << "write buffer success" << std::endl;
  auto read_buf = ins->GetBufferManager().ConstructManagedBuffer(block_size, nullptr, FileBufferType::MANAGED_BUFFER);
  auto buf2 = block_manager->ReadTemporaryBuffer(1, std::move(read_buf));
  std::cout << "read buffer success" << std::endl;
  // verify the data
  auto internal2 = buf2->InternalBuffer();
  auto ret = memcmp(internal2, internal, block_size);
  REQUIRE(ret == 0);
  std::cout << "verify buffer success" << std::endl;
}