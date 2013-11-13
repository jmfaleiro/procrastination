// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun Ren <kun@cs.yale.edu>
//
//

#ifndef CALVIN_COMMON_ATOMIC_H_
#define CALVIN_COMMON_ATOMIC_H_

#include <map>
#include <queue>
#include <vector>
#include "mutex.h"
#include <stdint.h>

using std::map;
using std::queue;
using std::vector;

// Queue with atomic push and pop operations. One Push and one Pop can occur
// concurrently, so consumers will not block producers, and producers will not
// block consumers.
//
// Note: Elements are not guaranteed to have their destructors called
//       immeduately upon removal from the queue.
//
template<typename T>
class AtomicQueue {
 public:
  AtomicQueue() {
    queue_.resize(256);
    size_ = 256;
    front_ = 0;
    back_ = 0;
  }

  // Returns the number of elements currently in the queue.
  inline size_t Size() {
    Lock l(&size_mutex_);
    return (back_ + size_ - front_) % size_;
  }

  // Returns true iff the queue is empty.
  inline bool Empty() {
    return front_ == back_;
  }

  // Atomically pushes 'item' onto the queue.
  inline void Push(const T& item) {
    Lock l(&back_mutex_);
    // Check if the buffer has filled up. Acquire all locks and resize if so.
    if (front_ == (back_+1) % size_) {
      Lock m(&front_mutex_);
      Lock n(&size_mutex_);
      uint32_t count = (back_ + size_ - front_) % size_;
      queue_.resize(size_ * 2);
      for (uint32_t i = 0; i < count; i++) {
        queue_[size_+i] = queue_[(front_ + i) % size_];
      }
      front_ = size_;
      back_ = size_ + count;
      size_ *= 2;
    }
    // Push item to back of queue.
    queue_[back_] = item;
    back_ = (back_ + 1) % size_;
  }

  // If the queue is non-empty, (atomically) sets '*result' equal to the front
  // element, pops the front element from the queue, and returns true,
  // otherwise returns false.
  inline bool Pop(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      front_ = (front_ + 1) % size_;
      return true;
    }
    return false;
  }

 private:
  vector<T> queue_;  // Circular buffer containing elements.
  uint32_t size_;      // Allocated size of queue_, not number of elements.
  uint32_t front_;     // Offset of first (oldest) element.
  uint32_t back_;      // First offset following all elements.

  // Mutexes for synchronization.
  Mutex front_mutex_;
  Mutex back_mutex_;
  Mutex size_mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicQueue(const AtomicQueue<T>&);
  AtomicQueue& operator=(const AtomicQueue<T>&);
};

// Atomic wrapper around a map<K, V*>. Exposes stl-like [] operator to allow
// low-contention lookups of elements that are guaranteed not to disappear out
// from under you afterwards (unless someone explicitly Erase()s an element).
template<typename K, typename V>
class AtomicPtrMap {
 public:
  AtomicPtrMap() {}
  ~AtomicPtrMap() {
    WriteLock l(&mutex_);
    for (typename std::map<K, V*>::iterator it = map_.begin(); it != map_.end();
         ++it) {
      delete it->second;
    }
  }

  inline void Erase(const K& k) {
    WriteLock l(&mutex_);
    if (map_.count(k)) {
      delete map_[k];
      map_.erase(k);
    }
  }

  inline V* Take(const K& k) {
    WriteLock l(&mutex_);
    if (map_.count(k)) {
      V* v = map_[k];
      map_.erase(k);
      return v;
    }
    return NULL;
  }

  inline V* Lookup(const K& k) {
    ReadLock l(&mutex_);
    if (map_.count(k) == 0) {
      return NULL;
    }
    return map_[k];
  }

  // Inserts element (k, v).
  // The AtomicPtrMap takes ownership of '*v'.
  //
  // Requires: No element exists yet with key 'k'.
  inline void Put(const K& k, V* v) {
    CHECK(Lookup(k) == NULL);
    WriteLock l(&mutex_);
    map_[k] = v;
  }

  inline V* operator[](const K& k) {
    V* v = Lookup(k);
    if (v == NULL) {
      WriteLock l(&mutex_);
      map_[k] = v = new V();
    }
    return v;
  }

 private:
  map<K, V*> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicPtrMap(const AtomicPtrMap<K, V>&);
  AtomicPtrMap& operator=(const AtomicPtrMap<K, V>&);
};

#endif  // CALVIN_COMMON_ATOMIC_H_

