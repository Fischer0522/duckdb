# pragma once
#include "duckdb/storage/temporary_file_manager.hpp"

namespace duckdb {
  static TemporaryBufferSize MinimumCompressedTemporaryBufferSize();
  
  static TemporaryBufferSize MaximumCompressedTemporaryBufferSize();
  
  bool TemporaryBufferSizeIsValid(const TemporaryBufferSize size);
  
  static TemporaryBufferSize SizeToTemporaryBufferSize(const idx_t size);
  
  static idx_t TemporaryBufferSizeToSize(const TemporaryBufferSize size);
  
  static TemporaryBufferSize RoundUpSizeToTemporaryBufferSize(const idx_t size);
  
  } // namespace duckdb