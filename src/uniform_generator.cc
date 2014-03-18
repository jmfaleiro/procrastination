#include "uniform_generator.h"
#include <simple_action.hh>
#include <algorithm>

using namespace simple;

UniformGenerator::UniformGenerator(int read_size, 
                                   int write_size, 
                                   int num_records,
                                   int freq) {

        
    // Size of read/write sets to generate. 
    m_read_set_size = read_size;
    m_write_set_size = write_size;
    m_num_records = num_records;
    m_freq = freq;
    m_use_next = 0;
    //    m_num_actions = 10000000;
    //    m_action_set = new Action[m_num_actions];
    //    memset(m_action_set, 0, sizeof(Action)*m_num_actions);
    //    m_use_next = 0;

    // Init rand number generator. 
    srand(time(NULL));
    gen_perfect_set(7);
}

void
UniformGenerator::gen_perfect_set(int num_threads) {
    m_perfect_set = (uint64_t**)malloc(sizeof(uint64_t*)*num_threads);
    memset(m_perfect_set, 0, sizeof(uint64_t*)*num_threads);
    std::set<int> done;

    for (int i = 0; i < num_threads; ++i) {
        m_perfect_set[i] = (uint64_t*)malloc(sizeof(uint64_t)*m_write_set_size);
        for (int j = 0; j < m_write_set_size; ++j) {
            m_perfect_set[i][j] = (uint64_t)genUnique(&done);
        }
    }
}


int UniformGenerator::genUnique(std::set<int>* done) {
    int record = -1;
    while (true) {
        record = rand() % m_num_records;
        if (done->find(record) == done->end()) {
            done->insert(record);
            return record;
        }
    }
}

Action* UniformGenerator::genNext() {

    uint64_t worker = m_use_next % 7;
    m_use_next += 1;

    SimpleAction* ret = new SimpleAction();
    for (int i = 0; i < m_write_set_size; ++i) {
        struct DependencyInfo to_add;
        to_add.record.m_table = 0;
        to_add.record.m_key = m_perfect_set[worker][i];
        ret->writeset.push_back(to_add);
    }
    /*
    ret->start_time = 0;
    ret->end_time = 0;
    ret->system_start_time = 0;
    ret->system_end_time = 0;
    */
    
    
    m_use_next += 1;
    ret->is_blind = false;
    
    // Generate elements to read. 
    /*
    for (int i = 0; i < m_read_set_size; ++i) {
        int record = genUnique(&done);
        struct DependencyInfo to_add;
        to_add.record.m_table = 0;
        to_add.record.m_key = record;
        ret->readset.push_back(to_add);
    }
    
    // Generate elements to write. 
    for (int i = 0; i < m_write_set_size; ++i) {
      int record = genUnique(&done);
      struct DependencyInfo to_add;
      to_add.record.m_table = 0;
      to_add.record.m_key = record;
      ret->writeset.push_back(to_add);
    }    
    */
    if ((rand() % m_freq) == 0) {
		ret->materialize = true;
    }
    else {
		ret->materialize = false;
    }
    return ret;    
}

EagerUniformGenerator::EagerUniformGenerator(int read_size, 
                                             int write_size, 
                                             int num_records,
                                             int freq) {
        
    // Size of read/write sets to generate. 
    m_read_set_size = read_size;
    m_write_set_size = write_size;
    m_num_records = num_records;
    m_freq = freq;
    m_last_used = 0;

    // Init rand number generator. 
    srand(time(NULL));    
}

int EagerUniformGenerator::genUnique(std::set<int>* done) {
    int record = -1;
    while (true) {
        record = rand() % m_num_records;
        if (done->find(record) == done->end()) {
            done->insert(record);
            return record;
        }
    }
}

EagerAction* EagerUniformGenerator::genNext() {

    std::set<int> done;
    SimpleEagerAction* ret = new SimpleEagerAction();
    /*
    ret->start_time = 0;
    ret->end_time = 0;
    ret->system_start_time = 0;
    ret->system_end_time = 0;
    */

    // Generate elements to read. 
    for (int i = 0; i < m_read_set_size; ++i) {
        int record = genUnique(&done);
        struct EagerRecordInfo to_add;
        to_add.record.m_table = 0;
        to_add.record.m_key = record;
        ret->readset.push_back(to_add);
    }
    
    // Generate elements to write.     
    for (int i = 0; i < m_write_set_size; ++i) {
        //        int record = 100000*i + m_last_used % 8;
        int record = genUnique(&done);
      struct EagerRecordInfo to_add;
      to_add.record.m_table = 0;
      to_add.record.m_key = record;
      ret->writeset.push_back(to_add);
    }    
    m_last_used += 1;
    std::sort(ret->readset.begin(), ret->readset.end());
    std::sort(ret->writeset.begin(), ret->writeset.end());
    return ret;    
}

