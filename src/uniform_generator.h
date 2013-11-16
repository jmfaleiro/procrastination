#ifndef UNIFORM_GENERATOR_H
#define UNIFORM_GENERATOR_H

#include "workload_generator.h"
#include <stdlib.h>
#include <time.h>
#include <set>

class UniformGenerator : public WorkloadGenerator {
    
    // Generator specific parameters.
    int m_read_set_size;
    int m_write_set_size;
    int m_num_records;
    int m_freq;

    virtual int genUnique(std::set<int>* done);

 public:
    UniformGenerator(int read_size, 
                     int write_size, 
                     int num_records,
                     int freq);

    virtual Action* genNext();
};


#endif // UNIFORM_GENERATOR_H
