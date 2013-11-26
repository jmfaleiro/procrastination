#ifndef ACTION_H
#define ACTION_H

#include <vector>
#include <stdint.h>

class Action;

struct DependencyInfo {
	int record;
	Action* dependency;
	bool is_write;
	int index;
};

class Action {
 public:
	int state;
	bool materialize;
        volatile uint64_t start_time;
        volatile uint64_t end_time;
	std::vector<struct DependencyInfo> readset;
	std::vector<struct DependencyInfo> writeset;
};

#endif // ACTION_H
