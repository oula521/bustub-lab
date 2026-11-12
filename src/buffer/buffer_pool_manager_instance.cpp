//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager_instance.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <array>
#include <cstring>
#include <vector>

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.push_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/* ============================== C1: 许洪娇 ============================== */

/**
 * Allocate a new page.
 *
 * The selected frame is reserved while latch_ is held. If a victim exists,
 * its page-table mapping is removed and its page ID is marked pending. Disk I/O
 * happens only after latch_ has been released.
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {

  if (page_id == nullptr) {
    return nullptr;
  }

  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool from_free_list = false;
  page_id_t old_page_id = INVALID_PAGE_ID;
  bool old_page_dirty = false;

  if (!free_list_.empty()) {
    from_free_list = true;
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }

    Page *old_page = &pages_[frame_id];
    old_page_id = old_page->page_id_;
    old_page_dirty = old_page->is_dirty_;

    if (old_page_id != INVALID_PAGE_ID) {
      pending_page_ids_.insert(old_page_id);
      page_table_->Remove(old_page_id);
    }

    // Evict() removed the frame's replacer history. The reserved frame is
    // deliberately in neither the free list nor the replacer.
    old_page->page_id_ = INVALID_PAGE_ID;
    old_page->pin_count_ = 1;
    old_page->is_dirty_ = false;
  }

  Page *page = &pages_[frame_id];
  if (from_free_list) {
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
  }

  const page_id_t new_page_id = AllocatePage();
  lock.unlock();

  try {
    // Preserve the victim's bytes until its dirty contents have been written.
    if (old_page_dirty) {
      page->RLatch();
      try {
        disk_manager_->WritePage(old_page_id, page->GetData());
      } catch (...) {
        page->RUnlatch();
        throw;
      }
      page->RUnlatch();
    }

    page->WLatch();
    page->ResetMemory();
    page->WUnlatch();
  } catch (...) {
    // Roll back the reservation. The old page data has not been reset unless
    // the write-back succeeded; in either case the in-memory bytes are intact
    // until ResetMemory(), which is reached only after successful write-back.
    lock.lock();

    if (old_page_id != INVALID_PAGE_ID) {
      page->page_id_ = old_page_id;
      page->pin_count_ = 0;
      page->is_dirty_ = old_page_dirty;
      page_table_->Insert(old_page_id, frame_id);

      // Evict() erased its history. Re-register the restored resident frame.
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, true);

      pending_page_ids_.erase(old_page_id);
    } else if (from_free_list) {
      page->page_id_ = INVALID_PAGE_ID;
      page->pin_count_ = 0;
      page->is_dirty_ = false;
      free_list_.push_back(frame_id);
    } else {
      // Defensive recovery for an unexpected invalid page ID in a victim frame.
      page->page_id_ = INVALID_PAGE_ID;
      page->pin_count_ = 0;
      page->is_dirty_ = false;
      free_list_.push_back(frame_id);
    }

    pending_cv_.notify_all();
    *page_id = INVALID_PAGE_ID;
    throw;
  }

  lock.lock();
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_->Insert(new_page_id, frame_id);

  // Keep the agreed call order: RecordAccess before SetEvictable(false).
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  if (old_page_id != INVALID_PAGE_ID) {
    pending_page_ids_.erase(old_page_id);
  }
  pending_cv_.notify_all();

  *page_id = new_page_id;
  return page;
}

/**
 * Fetch an existing page.
 *
 * A per-page pending marker prevents two threads from loading the same page
 * into different frames. A victim frame is reserved until I/O commits.
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  std::unique_lock<std::mutex> lock(latch_);

  while (pending_page_ids_.find(page_id) != pending_page_ids_.end()) {
    pending_cv_.wait(lock);
  }

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  page_id_t old_page_id = INVALID_PAGE_ID;
  bool old_page_dirty = false;
  std::array<char, BUSTUB_PAGE_SIZE> old_page_data{};

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    Page *old_page = &pages_[frame_id];
    old_page_id = old_page->page_id_;
    old_page_dirty = old_page->is_dirty_;

    if (old_page_id != INVALID_PAGE_ID) {
      pending_page_ids_.insert(old_page_id);
      page_table_->Remove(old_page_id);
    }
  }

  Page *page = &pages_[frame_id];

  // Reserve this frame and the requested page ID before releasing latch_.
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  pending_page_ids_.insert(page_id);

  lock.unlock();

  try {
    if (old_page_id != INVALID_PAGE_ID) {
      // Keep a copy so a failed disk read can restore the evicted in-memory page.
      page->RLatch();
      std::memcpy(old_page_data.data(), page->GetData(), BUSTUB_PAGE_SIZE);

      if (old_page_dirty) {
        try {
          disk_manager_->WritePage(old_page_id, page->GetData());
        } catch (...) {
          page->RUnlatch();
          throw;
        }
      }
      page->RUnlatch();
    }

    page->WLatch();
    try {
      page->ResetMemory();
      disk_manager_->ReadPage(page_id, page->GetData());
    } catch (...) {
      if (old_page_id != INVALID_PAGE_ID) {
        std::memcpy(page->GetData(), old_page_data.data(), BUSTUB_PAGE_SIZE);
      }
      page->WUnlatch();
      throw;
    }

    page->WUnlatch();
  } catch (...) {
    // Restore victim bytes before reacquiring the BPM latch. This avoids
    // waiting for a Page latch while holding latch_.
    if (old_page_id != INVALID_PAGE_ID) {
      page->WLatch();
      std::memcpy(page->GetData(), old_page_data.data(), BUSTUB_PAGE_SIZE);
      page->WUnlatch();
    }

    lock.lock();

    if (old_page_id != INVALID_PAGE_ID) {
      // Restore the victim's metadata, then make its frame evictable.
      page->page_id_ = old_page_id;
      page->pin_count_ = 0;
      page->is_dirty_ = old_page_dirty;
      page_table_->Insert(old_page_id, frame_id);

      // Evict() erased the old history; re-register the restored frame.
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, true);
      pending_page_ids_.erase(old_page_id);
    } else {
      // A frame selected by Evict() should always contain a valid page ID,
      // but return it to the free list defensively if that invariant is broken.
      page->page_id_ = INVALID_PAGE_ID;
      page->pin_count_ = 0;
      page->is_dirty_ = false;
      page->ResetMemory();
      free_list_.push_back(frame_id);
    }

    pending_page_ids_.erase(page_id);
    pending_cv_.notify_all();
    throw;
  }

  lock.lock();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_->Insert(page_id, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  pending_page_ids_.erase(page_id);
  if (old_page_id != INVALID_PAGE_ID) {
    pending_page_ids_.erase(old_page_id);
  }
  pending_cv_.notify_all();

  return page;
}

/* ============================== C2: 赵彤 ============================== */

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock<std::mutex> lock(latch_);


  while (pending_page_ids_.find(page_id) != pending_page_ids_.end()) {
    pending_cv_.wait(lock);
  }

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
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::unique_lock<std::mutex> lock(latch_);

  while (pending_page_ids_.find(page_id) != pending_page_ids_.end()) {
    pending_cv_.wait(lock);
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  // A temporary pin protects the frame. SetEvictable(false) is also required
  // because LRUKReplacer does not inspect Page::pin_count_ by itself.
  page->pin_count_++;
  replacer_->SetEvictable(frame_id, false);
  lock.unlock();

  page->RLatch();
  try {
    // Flush regardless of the dirty flag, as required by the BPM interface.
    disk_manager_->WritePage(page_id, page->GetData());
  } catch (...) {
    page->RUnlatch();

    lock.lock();
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    throw;
  }

  // Keep the page read-latched until the flush state is committed. Page writers
  // cannot change the bytes between WritePage() and clearing is_dirty_.
  lock.lock();
  page->is_dirty_ = false;
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  lock.unlock();

  page->RUnlatch();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::vector<page_id_t> page_ids;
  {
    std::scoped_lock<std::mutex> lock(latch_);
    page_ids.reserve(pool_size_);
    for (size_t i = 0; i < pool_size_; i++) {
      const page_id_t page_id = pages_[i].GetPageId();
      if (page_id != INVALID_PAGE_ID) {
        page_ids.push_back(page_id);
      }
    }
  }

  // Each FlushPgImp independently pins its frame and performs I/O without
  // holding latch_. A concurrently evicted dirty page has already been written
  // back by NewPgImp/FetchPgImp.
  for (const page_id_t page_id : page_ids) {
    FlushPgImp(page_id);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  while (pending_page_ids_.find(page_id) != pending_page_ids_.end()) {
    pending_cv_.wait(lock);
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  // pin_count_ == 0 means no client or FlushPgImp operation owns this frame.
  // Do not acquire the Page latch while holding latch_: FlushPgImp may hold a
  // Page read latch while briefly reacquiring latch_ to commit its flush.
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub