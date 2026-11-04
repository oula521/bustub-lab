#include "buffer/lru_k_replacer.h"
#include <stdexcept>
#include <utility>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id == nullptr) return false;

  bool found = false;
  frame_id_t victim = -1;
  std::pair<int, size_t> best{0, 0};

  auto rank = [&](frame_id_t fid) -> std::pair<int, size_t> {
    auto it = history_.find(fid);
    if (it == history_.end() || it->second.empty()) return {0, 0};
    const auto &history = it->second;
    bool has_inf = history.size() < k_;
    return {has_inf ? 0 : 1, history.front()};
  };

  for (const auto &[fid, evictable] : evictable_) {
    if (!evictable) continue;
    auto cur = rank(fid);
    if (!found || cur < best || (cur == best && fid < victim)) {
      found = true;
      victim = fid;
      best = cur;
    }
  }

  if (!found) return false;

  *frame_id = victim;
  evictable_.erase(victim);
  history_.erase(victim);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("frame_id out of range");
  }
  auto &history = history_[frame_id];
  history.push_back(current_timestamp_++);
  if (history.size() > k_) history.pop_front();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("frame_id out of range");
  }

  if (history_.find(frame_id) == history_.end()) {
    return;
  }

  if (set_evictable) {
    evictable_[frame_id] = true;
  } else {
    evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  evictable_.erase(frame_id);
  history_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return evictable_.size();
}

}  // namespace bustub