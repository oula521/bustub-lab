//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }

    Page *old_page = &pages_[frame_id];
    page_table_->Remove(old_page->page_id_);
    if (old_page->is_dirty_) {
      disk_manager_->WritePage(old_page->page_id_, old_page->GetData());
    }
  }

  // AllocatePage() is called only after a usable frame has been obtained.
  const page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;

  Page *new_page = &pages_[frame_id];
  new_page->ResetMemory();
  new_page->page_id_ = new_page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;

  page_table_->Insert(new_page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  std::scoped_lock<std::mutex> lock(latch_);

  // Cache hit.
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // Cache miss: prefer the free list, then use LRU-K.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    Page *old_page = &pages_[frame_id];
    page_table_->Remove(old_page->page_id_);
    if (old_page->is_dirty_) {
      disk_manager_->WritePage(old_page->page_id_, old_page->GetData());
    }
  }

  Page *page = &pages_[frame_id];
  page->ResetMemory();
  disk_manager_->ReadPage(page_id, page->GetData());

  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_->Insert(page_id, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

// These methods remain assigned to the other C-group member.
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { return false; }
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { return false; }
void BufferPoolManagerInstance::FlushAllPgsImp() {}
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
