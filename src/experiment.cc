#include <experiment.hh>

Experiment::Experiment(ExperimentInfo *info) {
    m_info = info;
}

SimpleQueue**
Experiment::InitQueues(int num_queues, uint32_t size) {
    assert(!(size & (size-1)));
    SimpleQueue **input_queues = new SimpleQueue*[m_info->num_workers];
    assert(input_queues != NULL);
    for (int i = 0; i < m_info->num_workers; ++i) {
        char *raw_queue_data = (char*)malloc(CACHE_LINE*size);
        input_queues[i] = new SimpleQueue(raw_queue_data, size);
        assert(input_queues[i] != NULL);
    }
    return input_queues;
}

timespec
Experiment::diff_time(timespec end, timespec start) {
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    }
    else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}

void
Experiment::Run() {
    using namespace tpcc;
    switch (m_info->experiment) {
    case TPCC:        
        TPCCInit tpcc_initializer(m_info->warehouses, m_info->districts, 
                                  m_info->customers, m_info->items);
        tpcc_initializer.do_init();
        RunTPCC();
        break;
    }
}
