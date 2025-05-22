#include <cstring>
#include <iostream>
#include "catch.hpp"
#include "block_manager.h"
#include "config.h"

using namespace mpool;
void print_test_result(const std::string& test_name, bool success) {
  std::cout << "Test: " << test_name << " - " 
            << (success ? "PASSED" : "FAILED") << std::endl;
}
