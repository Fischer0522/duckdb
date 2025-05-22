#include <cstring>
#include <iostream>
#include "catch.hpp"
#include "block_manager.h"
#include "common.h"
#include "config.h"

using mpool::BlockManager;
using mpool::BlockId;
void print_test_result(const std::string& test_name, bool success) {
  std::cout << "Test: " << test_name << " - " 
            << (success ? "PASSED" : "FAILED") << std::endl;
}
bool test_basic_read_write(BlockManager* block_manager) {
  BlockId id = 123;
  uint64_t block_size = 1024;
  unsigned char* content = new unsigned char[block_size];
  
  // Fill with pattern
  for (uint64_t i = 0; i < block_size; ++i) {
    content[i] = i % 256;
  }
  
  auto ret = block_manager->put(id, content, block_size);
  if (!ret.is_ok()) {
    std::cout << "Put failed with error code: " << ret.message() << std::endl;
    delete[] content;
    return false;
  }
  
  unsigned char* buffer = new unsigned char[block_size];
  ret = block_manager->get(id, buffer);
  
  if (!ret.is_ok()) {
    std::cout << "Get failed with error code: " << ret.message() << std::endl;
    delete[] content;
    delete[] buffer;
    return false;
  }
  
  // Verify data
  bool data_match = (memcmp(content, buffer, block_size) == 0);
  
  delete[] content;
  delete[] buffer;
  
  return data_match;
}

TEST_CASE("Test block manager init", "[mem_pool]") {
  auto config = new mpool::Config();
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
  std::cout <<std::endl;
  std::cout << "Initializing RemoteBlockManager..." << std::endl;
  std::unique_ptr<BlockManager> block_manager = 
      std::make_unique<BlockManager>(config);
  auto ret = block_manager->init();
  if (!ret.is_ok()) {
    std::cout << "RemoteBlockManager init failed with error code: " 
              << ret.message();
    return;
  }
  
  std::cout << "Running tests..." << std::endl;
  
  // // Run basic test
  bool basic_test_result = test_basic_read_write(block_manager.get());
  print_test_result("Basic Read/Write", basic_test_result);
  delete config;
}