#include <experiment.hh>
#include <simple_action.hh>
#include <algorithm>

using namespace std;

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
    TPCCInit *tpcc_initializer = new TPCCInit(m_info->warehouses, 
                                              m_info->districts, 
                                              m_info->customers, m_info->items);
    switch (m_info->experiment) {
    case TPCC:        
        tpcc_initializer->do_init();
        RunTPCC();
        break;
        
    case THROUGHPUT:
        simple::do_simple_init(m_info->num_records);
        assert(simple::s_simple_table != NULL);
        RunThroughput();
        break;

    case PEAK_LOAD:
        simple::do_simple_init(m_info->num_records);
        assert(simple::s_simple_table != NULL);
        RunPeak();
        break;
    case BLIND:
        shopping::do_shopping_init(2000, 1000000);
        RunBlind();
        break;
    }
}

void
Experiment::WriteThroughput(timespec time, uint32_t num_processed) {
    ofstream throughput_file;
    throughput_file.open(m_info->throughput_file.c_str(), ios::app | ios::out);
    throughput_file << time.tv_sec << "." << time.tv_nsec;
    throughput_file <<  "  " << num_processed << "\n";
    throughput_file.close();
}

void
Experiment::WriteCDF(double *times, int count) {
    ofstream cdf_file;
    cdf_file.open(m_info->latency_file.c_str(), ios::out);

    std::sort(&times[0], &times[count]);
    double fraction = 0.0;
    double diff = 1.0/(count*1.0);    
    for (int i = 0; i < count; ++i) {
        cdf_file << fraction << " " << times[i] << "\n";
        fraction  += diff;        
    }
    cdf_file.close();
}
