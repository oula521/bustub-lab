//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { return nullptr; }

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { return nullptr; }


auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;

  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}




auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  // 临时固定 Frame，防止 I/O 期间被淘汰
  page->pin_count_++;
  replacer_->SetEvictable(frame_id, false);

  // 预先清除 Dirty，避免覆盖 I/O 期间产生的新脏标记
  page->is_dirty_ = false;

  lock.unlock();

  // 使用页面读锁保护写盘时读取的数据
  page->RLatch();
  disk_manager_->WritePage(page_id, page->GetData());
  page->RUnlatch();

  // 恢复 Frame 的固定状态
  lock.lock();

  page->pin_count_--;

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}



void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    page_id_t page_id;

    {
      std::scoped_lock<std::mutex> lock(latch_);
      page_id = pages_[i].GetPageId();
    }

    if (page_id != INVALID_PAGE_ID) {
      FlushPgImp(page_id);
    }
  }
}



auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    return false;
  }

  // A组：删除 Page ID 到 Frame ID 的映射
  page_table_->Remove(page_id);

  // B组：删除 Frame 的替换记录
  replacer_->Remove(frame_id);

  // 重置页面数据和元数据
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  // 回收 Frame
  free_list_.push_back(frame_id);

  DeallocatePage(page_id);

  return true;
}


auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
