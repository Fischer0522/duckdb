#include "duckdb/storage/remote_block_manager.hpp"
#include "block_manager.h"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "zstd.h"
#include "duckdb/storage/block_tracker.hpp"
#include <mutex>

namespace duckdb {
RemoteBlockManager::RemoteBlockManager(DatabaseInstance& db)
  : db(db), block_manager(nullptr) {
  config = make_uniq<mpool::Config>();
  enable_compress = true;
  config->hn_host = "127.0.0.1";
  config->hn_user = "root";
  config->hn_password = "123123";
  config->hn_port = 2250;
  config->pmd_id = 1;
  // 2GB remote memory and 512MB (4 chunks) local memory
  config->remote_memory_size = 2147483648;
  config->local_memory_size = static_cast<uint64_t>(640 * 1024 * 1024);
  // 32K, 64K, 96K, 128K, 160K, 192K, 224K, 256K
  config->alloc_sizes = {32768, 65536, 98304, 131072, 
    163840, 196608, 229376, 262144};
  config->eviction_strategy = "lru";
  config->enable_flusher = true;
  new (&block_manager) mpool::BlockManager(config.get());
  auto ret = block_manager.init();
  if (!ret.is_ok()) {
    throw InternalException("RemoteBlockManager init failed with error code: " + ret.message());
  }
}

RemoteBlockManager::~RemoteBlockManager() {
}

static TemporaryBufferSize MinimumCompressedRemoteBufferSize() {
	return TemporaryBufferSize::S32K;
}

static TemporaryBufferSize MaximumCompressedRemoteBufferSize() {
	return TemporaryBufferSize::S224K;
}

bool RemoteBufferSizeIsValid(const TemporaryBufferSize size) {
	switch (size) {
	case TemporaryBufferSize::S32K:
	case TemporaryBufferSize::S64K:
	case TemporaryBufferSize::S96K:
	case TemporaryBufferSize::S128K:
	case TemporaryBufferSize::S160K:
	case TemporaryBufferSize::S192K:
	case TemporaryBufferSize::S224K:
	case TemporaryBufferSize::DEFAULT:
		return true;
	default:
		return false;
	}
}

static TemporaryBufferSize SizeToRemoteBufferSize(const idx_t size) {
	D_ASSERT(size != 0 && size % TEMPORARY_BUFFER_SIZE_GRANULARITY == 0);
	const auto res = static_cast<TemporaryBufferSize>(size);
	D_ASSERT(RemoteBufferSizeIsValid(res));
	return res;
}

static idx_t RemoteBufferSizeToSize(const TemporaryBufferSize size) {
	D_ASSERT(RemoteBufferSizeIsValid(size));
	return static_cast<idx_t>(size);
}

static TemporaryBufferSize RoundUpSizeToRemoteBufferSize(const idx_t size) {
	return SizeToRemoteBufferSize(AlignValue<idx_t, TEMPORARY_BUFFER_SIZE_GRANULARITY>(size));
}

void RemoteBlockManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
  D_ASSERT(buffer.AllocSize() == BufferManager::GetBufferManager(db).GetBlockAllocSize());
  if (!enable_compress) {
    auto ret = block_manager.put(block_id, buffer.InternalBuffer(), buffer.AllocSize());
    if (!ret.is_ok()) {
      throw InternalException("RemoteBlockManager::WriteTemporaryBuffer failed with : " + ret.message());
    }
    std::unique_lock<mutex> guard(manager_block);
    block_sizes[block_id] = buffer.AllocSize();
    return;
  }
  const auto adaptivity_idx = TaskScheduler::GetEstimatedCPUId() % COMPRESSION_ADAPTIVITIES;
	auto &compression_adaptivity = compression_adaptivities[adaptivity_idx];

	const auto time_before_ns = TemporaryFileCompressionAdaptivity::GetCurrentTimeNanos();
	AllocatedData compressed_buffer;
	const auto compression_result = CompressBuffer(compression_adaptivity, buffer, compressed_buffer);
  auto &config = DBConfig::GetConfig(db);
  if (config.options.track_block_access) {
		try {
			BlockTracker::GetInstance(db).TrackWrite(block_id, RemoteBufferSizeToSize(compression_result.size), "WriteTemporaryBuffer");
		} catch (...) {
				// Ignore exceptions in tracking to not affect normal operation
		}
	}
	compression_adaptivity.Update(compression_result.level, time_before_ns);

  if (compression_result.size == TemporaryBufferSize::DEFAULT) {
    auto ret = block_manager.put(block_id, buffer.InternalBuffer(), buffer.AllocSize());
    if (!ret.is_ok()) {
      auto error = StringUtil::Format("Write failed, buffer size is %d, mes: %s", buffer.AllocSize(), ret.message());
      throw InternalException(error);
    }
  } else {
    auto ret = block_manager.put(block_id, compressed_buffer.get(), RemoteBufferSizeToSize(compression_result.size));
    if (!ret.is_ok()) {
      auto error = StringUtil::Format("Write failed after Compression, buffer size is %d, mes: %s", buffer.AllocSize(), ret.message());
      throw InternalException(error);
    }
  }
  std::unique_lock<mutex> guard(manager_block);
  block_sizes[block_id] = RemoteBufferSizeToSize(compression_result.size);
}

bool RemoteBlockManager::HasTemporaryBuffer(block_id_t block_id) {
  std::unique_lock<mutex> guard(manager_block);
  return block_manager.has(block_id) && block_sizes.find(block_id) != block_sizes.end();
}
unique_ptr<FileBuffer> RemoteBlockManager::ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
  uint64_t block_size;
  {
    std::unique_lock<mutex> lock(manager_block);
    if (block_sizes.find(id) == block_sizes.end()) {
      throw InternalException("Block not found in RemoteBlockManager");
    }
    block_size = block_sizes[id];
  }
  auto& buffer_manager = BufferManager::GetBufferManager(db);

  if (!enable_compress) {
    // reusable buffer might be nullptr, construct a new buffer here
    auto constructed_buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(), std::move(reusable_buffer));
    auto ret = block_manager.get(id,constructed_buffer->InternalBuffer());
    if (!ret.is_ok()) {
      throw InternalException("RemoteBlockManager::ReadTemporaryBuffer failed with : " + ret.message());
    }
    return constructed_buffer;
  }
  auto &config = DBConfig::GetConfig(db);
	if (config.options.track_block_access) {
		try {
				BlockTracker::GetInstance(db).TrackRead(id, block_size, "ReadTemporaryBuffer");
		} catch (...) {
				// Ignore exceptions in tracking to not affect normal operation
		}
	}

  if (SizeToRemoteBufferSize(block_size) == TemporaryBufferSize::DEFAULT) {
    // reusable buffer might be nullptr, construct a new buffer here
    auto constructed_buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(), std::move(reusable_buffer));
    auto ret = block_manager.get(id, constructed_buffer->InternalBuffer());
    if (!ret.is_ok()) {
      throw InternalException("RemoteBlockManager::ReadTemporaryBuffer failed with : " + ret.message());
    }
    EraseUsedBlock(id);
    return constructed_buffer;
  } 
  // handle compress here
  AllocatedData compressed_buffer = Allocator::Get(db).Allocate(block_size);
  auto ret = block_manager.get(id,compressed_buffer.get());
  if (!ret.is_ok()) {
    throw InternalException("RemoteBlockManager::ReadTemporaryBuffer failed with : " + ret.message());
  }

  auto buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(), std::move(reusable_buffer));
  auto decompressed_buffer = DecompressBuffer(std::move(buffer), compressed_buffer, block_size);
  EraseUsedBlock(id);
  return decompressed_buffer;
}

void RemoteBlockManager::DeleteTemporaryBuffer(block_id_t id) {
  EraseUsedBlock(id);
}

RemoteBlockManager::CompressionResult RemoteBlockManager::CompressBuffer(TemporaryFileCompressionAdaptivity &compression_adaptivity, FileBuffer &buffer,
  AllocatedData &compressed_buffer) {
  	if (buffer.AllocSize() <= RemoteBufferSizeToSize(MinimumCompressedRemoteBufferSize())) {
      // Buffer size is less or equal to the minimum compressed size - no point compressing
      return {TemporaryBufferSize::DEFAULT, TemporaryCompressionLevel::UNCOMPRESSED};
    }

    const auto level = compression_adaptivity.GetCompressionLevel();
    if (level == TemporaryCompressionLevel::UNCOMPRESSED) {
      return {TemporaryBufferSize::DEFAULT, TemporaryCompressionLevel::UNCOMPRESSED};
    }

    const auto compression_level = static_cast<int>(level);
    D_ASSERT(compression_level >= duckdb_zstd::ZSTD_minCLevel() && compression_level <= duckdb_zstd::ZSTD_maxCLevel());
    const auto zstd_bound = duckdb_zstd::ZSTD_compressBound(buffer.AllocSize());

    compressed_buffer = Allocator::Get(db).Allocate(sizeof(idx_t) + zstd_bound);
    const auto zstd_size = duckdb_zstd::ZSTD_compress(compressed_buffer.get() + sizeof(idx_t), zstd_bound,
                                                      buffer.InternalBuffer(), buffer.AllocSize(), compression_level);
    D_ASSERT(!duckdb_zstd::ZSTD_isError(zstd_size));
    Store<idx_t>(zstd_size, compressed_buffer.get());
    const auto compressed_size = sizeof(idx_t) + zstd_size;

    if (compressed_size > RemoteBufferSizeToSize(MaximumCompressedRemoteBufferSize())) {
      return {TemporaryBufferSize::DEFAULT, level}; // Use default size if compression ratio is bad
    }

    return {RoundUpSizeToRemoteBufferSize(compressed_size), level};
}

unique_ptr<FileBuffer> RemoteBlockManager::DecompressBuffer(unique_ptr<FileBuffer> reusable_buffer, AllocatedData& compressed_buffer, uint64_t block_size) {
  auto &buffer_manager = BufferManager::GetBufferManager(db);
  	// Decompress into buffer
	auto buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(), std::move(reusable_buffer));
  const auto compressed_size = Load<idx_t>(compressed_buffer.get());
	D_ASSERT(!duckdb_zstd::ZSTD_isError(compressed_size));
	const auto decompressed_size = duckdb_zstd::ZSTD_decompress(
	    buffer->InternalBuffer(), buffer->AllocSize(), compressed_buffer.get() + sizeof(idx_t), compressed_size);
	(void)decompressed_size;
	D_ASSERT(!duckdb_zstd::ZSTD_isError(decompressed_size));

	D_ASSERT(decompressed_size == buffer->AllocSize());
  return buffer;
}

void RemoteBlockManager::EraseUsedBlock(block_id_t id) {
  auto ret = block_manager.remove(id);
  if (!ret.is_ok()) {
    throw InternalException("RemoteBlockManager::DeleteTemporaryBuffer failed with : " + ret.message());
  }
  auto &config = DBConfig::GetConfig(db);
	if (config.options.track_block_access) {
		try {
			BlockTracker::GetInstance(db).TrackErase(id, 0, "EraseUsedBlock");
		} catch (...) {
      Printer::PrintF("Error in EraseUsedBlock, block_id : %llu", id);
				// Ignore exceptions in tracking to not affect normal operation
		}
	}
  std::unique_lock<mutex> lock(manager_block);
  block_sizes.erase(id);
}

} // namespace duckdb