#ifndef UNIFORM_GENERATOR_H
#define UNIFORM_GENERATOR_H

#include "workload_generator.h"
#include <stdlib.h>
#include <time.h>
#include <set>
#include <eager_generator.hh>

class UniformGenerator : public WorkloadGenerator {
    
    // Generator specific parameters.
    int m_read_set_size;
    int m_write_set_size;
    int m_num_records;
    int m_freq;

    uint64_t **m_perfect_set;

    virtual int genUnique(std::set<int>* done);

    virtual void gen_perfect_set(int num_threads);

 public:
    UniformGenerator(int read_size, 
                     int write_size, 
                     int num_records,
                     int freq);


    virtual Action* genNext();
};

class EagerUniformGenerator : public EagerGenerator {
    
    // Generator specific parameters.
    int m_read_set_size;
    int m_write_set_size;
    int m_num_records;
    int m_freq;

    int m_last_used;

    virtual int genUnique(std::set<int>* done);

 public:
    EagerUniformGenerator(int read_size, 
                          int write_size, 
                          int num_records,
                          int freq);


    virtual EagerAction* genNext();
};


#endif // UNIFORM_GENERATOR_H
