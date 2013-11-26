#ifndef WORKLOAD_GENERATOR_H
#define WORKLOAD_GENERATOR_H

#include "action.h"
#include <cstring>

class WorkloadGenerator {
 protected:
	Action* m_action_set;
	int m_use_next;

 public:

  // Generate a random action. 
  virtual Action* genNext() = 0;
  
  virtual int numUsed() {
      return m_use_next;
  }
  
  virtual Action* getActions() {
      return m_action_set;
  }
};

#endif // WORKLOAD_GENERATOR_H
