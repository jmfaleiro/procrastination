#include "workload_generator.h"
#include <sys/time.h>
#include <set>
#include <iostream>


WorkloadGenerator::WorkloadGenerator(int read_size, 
                                     int write_size, 
                                     int num_records,
                                     int freq) {

  // Size of read/write sets to generate. 
  m_read_set_size = read_size;
  m_write_set_size = write_size;
  m_num_records = num_records;
  m_freq = freq;
  
  // Init rand number generator. 
  srand(time(NULL));
}

Action*
WorkloadGenerator::genNext() {
  Action* ret = new Action();
  std::set<int> done;
  int record = -1;
  // Generate five elements to read. 
  for (int i = 0; i < m_read_set_size; ++i) {
      while (true) {
          record = rand() % m_num_records;
          if (done.find(record) == done.end()) {
              break;
          }
      }
      done.insert(record);
      ret->add_readset(record);
  }
  
  // Generate five elements to write. 
  for (int i = 0; i < m_write_set_size; ++i) {
      while (true) {
          record = rand() % m_num_records;
          if (done.find(record) == done.end()) {
              break;
          }
      }
      done.insert(record);
      ret->add_writeset(record);
  }


  if ((rand() % m_freq) == 0) {
      ret->set_materialize(true);
  }
  else {
      ret->set_materialize(false);
  }
  return ret;
}

