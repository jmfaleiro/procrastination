#ifndef NORMAL_GENERATOR_H
#define NORMAL_GENERATOR_H

#include "workload_generator.h"
#include <set>
#include <random>
#include <cassert>
#include <eager_generator.hh>

class NormalGenerator : public WorkloadGenerator {
    
    // Parameters for workload generation. 
    int m_read_set_size;
    int m_write_set_size;
    int m_num_records;
    int m_freq;
    int m_std_dev;

    virtual int genUnique(std::default_random_engine* generator,
                          std::normal_distribution<double>* dist,
                          std::set<int>* done);

 public:
    NormalGenerator(int read_size,
                    int write_size,
                    int num_records,
                    int freq,
                    int std_dev);


    
    virtual Action* genNext();
};

class EagerNormalGenerator : public EagerGenerator {
    
    // Parameters for workload generation. 
    int m_read_set_size;
    int m_write_set_size;
    int m_num_records;
    int m_freq;
    int m_std_dev;

    virtual int genUnique(std::default_random_engine* generator,
                          std::normal_distribution<double>* dist,
                          std::set<int>* done);

 public:
    EagerNormalGenerator(int read_size,
                         int write_size,
                         int num_records,
                         int freq,
                         int std_dev);


    
    virtual EagerAction* genNext();
};


#endif // NORMAL_GENERATOR_H
