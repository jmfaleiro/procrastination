#ifndef WORKLOAD_GENERATOR_H
#define WORKLOAD_GENERATOR_H

#include "action_int.pb.h"

class WorkloadGenerator {
  
  // Size of read/write sets.
  int m_read_set_size;
  int m_write_set_size;
  
  int m_num_records;
  
  int m_freq;

 public:
  WorkloadGenerator(int read_set_size, 
                    int write_set_size, 
                    int records, 
                    int freq);

  // Generate a random action. 
  Action* genNext();
};

#endif // WORKLOAD_GENERATOR_H
