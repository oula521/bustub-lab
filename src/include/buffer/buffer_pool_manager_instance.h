//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager_instance.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);
  ~BufferPoolManagerInstance() override;

  auto GetPoolSize() -> size_t override { return pool_size_; }
  auto GetPages() -> Page * { return pages_; }

 protected:
  auto NewPgImp(page_id_t *page_id) -> Page * override;
  auto FetchPgImp(page_id_t page_id) -> Page * override;
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;
  auto FlushPgImp(page_id_t page_id) -> bool override;
  void FlushAllPgsImp() override;
  auto DeletePgImp(page_id_t page_id) -> bool override;

  const size_t pool_size_;
  std::atomic<page_id_t> next_page_id_ = 0;
  const size_t bucket_size_ = 4;

  Page *pages_;
  DiskManager *disk_manager_ __attribute__((__unused__));
  LogManager *log_manager_ __attribute__((__unused__));
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;
  LRUKReplacer *replacer_;
  std::list<frame_id_t> free_list_;

  /**
   * Protects page_table_, free_list_, replacer_ coordination, pending_page_ids_,
   * and Page metadata (page_id_, pin_count_, is_dirty_).
   * Never hold this latch while performing disk I/O or waiting for a Page latch.
   */
  std::mutex latch_;

  /**
   * Page IDs currently being loaded into, or evicted from, a reserved frame.
   * A caller fetching/flushing/deleting one of these IDs waits until the
   * in-flight operation commits or rolls back.
   */
  std::condition_variable pending_cv_;
  std::unordered_set<page_id_t> pending_page_ids_;

  auto AllocatePage() -> page_id_t;

  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-op right now without a more complex data structure to track deallocated pages.
  }

  // TODO(student): You may add additional private members and helper functions.
};

}  // namespace bustub
