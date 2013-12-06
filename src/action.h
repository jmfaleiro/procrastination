#ifndef ACTION_H
#define ACTION_H

#include <vector>
#include <stdint.h>
#include "machine.h"

class Action;

struct DependencyInfo {
  int record;
  Action* dependency;
  bool is_write;
  int index;
};

class Action {
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
};

#endif // ACTION_H
