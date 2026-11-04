//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <stdexcept>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // 没有可淘汰的 Frame
  if (frame_id == nullptr || evictable_.empty()) {
    return false;
  }

  bool found = false;
  frame_id_t victim = 0;

  // 记录当前最优候选
  bool best_inf = false;
  size_t best_timestamp = 0;

  // 遍历所有 Frame
  for (const auto &[fid, is_evictable] : evictable_) {
    // 跳过不可淘汰的 Frame
    if (!is_evictable) {
      continue;
    }

    // 查找访问历史
    auto history_it = history_.find(fid);

    if (history_it == history_.end() || history_it->second.empty()) {
      continue;
    }

    const auto &history = history_it->second;

    // 判断访问次数是否不足 K 次
    bool is_inf = history.size() < k_;

    size_t timestamp = 0;

    if (is_inf) {
      // 不足 K 次，选择最早的一次访问时间
      timestamp = history.front();
    } else {
      // 找到倒数第 K 次访问时间
      auto it = history.rbegin();

      for (size_t i = 1; i < k_; i++) {
        ++it;
      }

      timestamp = *it;
    }

    // 判断当前 Frame 是否应该成为淘汰对象
    bool should_replace = false;

    if (!found) {
      should_replace = true;
    } else if (is_inf && !best_inf) {
      // 无穷大优先于有限距离
      should_replace = true;
    } else if (is_inf == best_inf) {
      // 同为无穷大或同为有限距离
      // 访问时间更早的优先淘汰
      if (timestamp < best_timestamp) {
        should_replace = true;
      } else if (timestamp == best_timestamp && fid < victim) {
        should_replace = true;
      }
    }

    if (should_replace) {
      found = true;
      victim = fid;
      best_inf = is_inf;
      best_timestamp = timestamp;
    }
  }

  // 没有找到可淘汰对象
  if (!found) {
    return false;
  }

  // 返回被淘汰的 Frame ID
  *frame_id = victim;

  // 删除该 Frame 的访问历史和淘汰状态
  history_.erase(victim);
  evictable_.erase(victim);

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame_id");
  }

  // Frame 必须已经有访问历史
  if (history_.find(frame_id) == history_.end()) {
    return;
  }

  // 检查当前状态
  bool old_status = evictable_.find(frame_id) != evictable_.end();

  // 状态没有变化，不做处理
  if (old_status == set_evictable) {
    return;
  }

  // 只保存允许淘汰的 Frame
  if (set_evictable) {
    evictable_[frame_id] = true;
  } else {
    evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
