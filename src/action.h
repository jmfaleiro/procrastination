#ifndef ACTION_H
#define ACTION_H

#include <vector>
#include <deque>
#include <stdint.h>

class Action;

struct ActionItem;

struct DependencyInfo {
	int record;
	Action* dependency;
	bool is_write;
	int index;
};

class Action {
 public:
  
  ActionItem* wakeups;
  int cpu;
  int wait_count;
  bool is_checkout;
  int state;
  int num_writes;
  bool materialize;
  bool is_blind;
  volatile uint64_t start_time;
  volatile uint64_t end_time;
  std::vector<struct DependencyInfo> readset;
  std::vector<struct DependencyInfo> writeset;
};

#endif // ACTION_H
