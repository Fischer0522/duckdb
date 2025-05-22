#pragma once

#include "block_manager.h"
#include "config.h"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
namespace duckdb {
class RemoteBlockManager {
public:
  explicit RemoteBlockManager(DatabaseInstance& db);
  ~RemoteBlockManager();
  void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	bool HasTemporaryBuffer(block_id_t block_id);
	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer);
	void DeleteTemporaryBuffer(block_id_t id);

  struct CompressionResult {
    TemporaryBufferSize size;
    TemporaryCompressionLevel level;
  };
  CompressionResult CompressBuffer(TemporaryFileCompressionAdaptivity &compression_adaptivity, FileBuffer &buffer,
    AllocatedData &compressed_buffer);

  unique_ptr<FileBuffer> DecompressBuffer(unique_ptr<FileBuffer> reusable_buffer, AllocatedData& compressed_buffer, uint64_t compressed_size);
  private:
  void EraseUsedBlock(block_id_t id);

  DatabaseInstance &db;
  
  mutex manager_block;

  unordered_map<block_id_t, uint64_t> block_sizes;

  unique_ptr<mpool::BlockManager> block_manager;

  unique_ptr<mpool::Config> config;

  atomic<idx_t> size_on_remote;

  bool enable_compress;
	//! How many compression adaptivities we have so that threads don't all share the same one
	static constexpr idx_t COMPRESSION_ADAPTIVITIES = 64;
	//! Class that oversees when/how much to compress
	array<TemporaryFileCompressionAdaptivity, COMPRESSION_ADAPTIVITIES> compression_adaptivities;
};
}