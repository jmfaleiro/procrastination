#ifndef WORKLOAD_GENERATOR_H
#define WORKLOAD_GENERATOR_H

#include "action.h"

class WorkloadGenerator {
 protected:
	Action* m_action_set;
	int m_use_next;

 public:

  // Generate a random action. 
  virtual Action* genNext() = 0;
};

#endif // WORKLOAD_GENERATOR_H
