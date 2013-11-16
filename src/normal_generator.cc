#include "normal_generator.h"

NormalGenerator::NormalGenerator(int read_size,
                                 int write_size,
                                 int num_records,
                                 int freq,
                                 int std_dev) {
    m_read_set_size = read_size;
    m_write_set_size = write_size;
    m_num_records = num_records;
    m_freq = freq;
    m_std_dev = std_dev;
    
    srand(time(NULL));
}

int NormalGenerator::genUnique(std::default_random_engine* generator,
                               std::normal_distribution<double>* dist,
                               std::set<int>* done) {
    int record = -1;
    while (true) {
        record = (((int)(*dist)(*generator)) + m_num_records) % m_num_records;
        assert(record >= 0 && record < m_num_records);
        if (done->find(record) == done->end()) {
            done->insert(record);
            
            return record;
        }
    }
}

Action* NormalGenerator::genNext() {
    // Make sure that we generate unique records in the read/write set. 
    std::set<int> done;
    Action* ret = new Action();

    // Pick a random median to start from. 
    int median = rand() % m_num_records;

    // Initialize the distribution generator. 
    std::default_random_engine generator;
    std::normal_distribution<double> distribution((double)median, 
                                                  (double)m_std_dev);
    
    // Generate the read and write sets. 
    for (int i = 0; i < m_read_set_size; ++i) {
        int record = genUnique(&generator, &distribution, &done);
        ret->add_readset(record);
    }
    for (int i = 0; i < m_write_set_size; ++i) {
        int record = genUnique(&generator, &distribution, &done);
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
