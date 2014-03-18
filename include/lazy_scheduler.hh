#ifndef 	LAZY_SCHEDULER_HH_
#define 	LAZY_SCHEDULER_HH_

#include <set>
#include <vector>
#include <deque>
#include <pthread.h>
#include <numa.h>
#include <tr1/unordered_map>
#include <list>
#include <vector>
#include <table.hh>
#include <action.h>
#include "concurrent_queue.h"
#include "cpuinfo.h"
#include "util.h"
#include <tpcc.hh>
#include <lazy_worker.hh>
#include <concurrency_control_params.hh>
#include <runnable.hh>

using namespace std;
using namespace cc_params;

class Heuristic {
public:
    Action* last_txn;
    int index;
    bool is_write;
    int chain_length;


    Heuristic() {
        last_txn = NULL;
        index = -1;
        is_write = false;
        chain_length = 0;
    }    
};

class LazyScheduler : public Runnable {
private:
    SimpleQueue 						*m_input_queue;
    SimpleQueue							**m_feedback_queues;
    SimpleQueue							**m_worker_queues;
    int 								m_num_workers;
    int 								m_cpu_number;
    Table<uint64_t, Heuristic>			**m_tables;    
    uint64_t 							m_last_used;
    int		 							m_max_chain;
    volatile uint64_t 					m_num_stickified;
    uint64_t 							m_materialize_counter;
    bool								m_materialize_on;
    //    bool 								m_is_tpcc;
    SimpleQueue 						*m_internal_queue;
    volatile uint64_t					m_internal_flag;
    pthread_t							m_pipeline_thread;
    int m_dummy;


    void
    AddGraph(Action *txn);
    

    static void*
    Pipeline(void *arg);


protected:
    virtual void
    StartWorking();

public:
    LazyScheduler(SimpleQueue *input_queue, SimpleQueue **feedback_queues, 
                  SimpleQueue **worker_queues, int num_workers, int cpu_number,
                  cc_params::TableInit *params, int num_params, 
                  int max_chain);

    uint64_t
    NumStickified();
};

#endif // LAZY_SCHEDULER_HH_
