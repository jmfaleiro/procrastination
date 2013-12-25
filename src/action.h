#ifndef ACTION_H
#define ACTION_H

#include <vector>
#include <stdint.h>
#include "machine.h"
#include "util.h"

class Action;

struct DependencyInfo {
  int record;
  Action* dependency;
  bool is_write;
  int index;
};

struct StateLock {
  uint64_t values[8];
} __attribute__ ((aligned(CACHE_LINE)));

class Action {

 private:
  volatile uint64_t values[8] __attribute__ ((aligned(CACHE_LINE)));

 public:  
  bool is_checkout;
  int state;
  int num_writes;
  bool materialize;
  int is_blind;
  volatile uint64_t start_time;
  volatile uint64_t end_time;
  volatile uint64_t system_start_time;
  volatile uint64_t system_end_time;
  std::vector<struct DependencyInfo> readset;
  std::vector<struct DependencyInfo> writeset;
  std::vector<int> real_writes;
  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_start_time;    
  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_end_time;    
  
  // Use the following two functions while performing graph traversals, to 
  // ensure mutual exclusion between threads that touch the same transaction. 
  static inline void Lock(Action* action) {

    // Try to set the first element of the values array to 1. 
    while (xchgq(&(action->values[0]), 1) == 1)
      do_pause();
  }
  static inline void Unlock(Action* action) {
    xchgq(&(action->values[0]), 0);
  }

  static inline void ChangeState(Action* action, uint64_t value) {
    xchgq(&(action->values[4]), value);
  }

  static inline uint64_t CheckState(Action* action) {
    return action->values[4];
  }

};

#endif // ACTION_H
