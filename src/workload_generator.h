#ifndef WORKLOAD_GENERATOR_H
#define WORKLOAD_GENERATOR_H

#include "action_int.pb.h"

class WorkloadGenerator {

 public:

  // Generate a random action. 
  virtual Action* genNext() = 0;
};

#endif // WORKLOAD_GENERATOR_H
