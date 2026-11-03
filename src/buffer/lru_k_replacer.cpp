#include "buffer/lru_k_replacer.h"

#include <stdexcept>
#include <utility>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id == nullptr) {
    return false;
  }

  bool found = false;
  frame_id_t victim = -1;
  std::pair<int, size_t> best{0, 0};

  auto rank = [&](frame_id_t fid) -> std::pair<int, size_t> {
    auto it = history_.find(fid);
    if (it == history_.end() || it->second.empty()) {
      return {0, 0};
    }

    const auto &history = it->second;
    bool has_inf = history.size() < k_;
    return {has_inf ? 0 : 1, history.front()};
  };

  for (const auto &[fid, evictable] : evictable_) {
    if (!evictable) {
      continue;
    }

    auto cur = rank(fid);
    if (!found || cur < best || (cur == best && fid < victim)) {
      found = true;
      victim = fid;
      best = cur;
    }
  }

  if (!found) {
    return false;
  }

  *frame_id = victim;
  --curr_size_;
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

  if (history.size() > k_) {
    history.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("frame_id out of range");
  }

  auto it = evictable_.find(frame_id);
  bool old_evictable = (it != evictable_.end() && it->second);

  if (old_evictable == set_evictable) {
    return;
  }

  evictable_[frame_id] = set_evictable;

  if (set_evictable) {
    ++curr_size_;
  } else {
    --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = evictable_.find(frame_id);
  if (it == evictable_.end()) {
    return; // 没找到直接返回
  }

  if (!it->second) {
    // 如果不可淘汰，抛出异常（严格遵循头文件注释）
    throw std::invalid_argument("Cannot remove a non-evictable frame");
  }

  --curr_size_;
  evictable_.erase(it);
  history_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub