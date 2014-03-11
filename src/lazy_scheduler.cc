#include "lazy_scheduler.hh"
#include "machine.h"
#include <tpcc.hh>
#include <assert.h>
#include <deque>
#include <bulk_allocating_table.hh>
#include <two_dim_table.hh>
#include <three_dim_table.hh>
#include <tpcc.hh>
#include <pthread.h>


using namespace tpcc;
using namespace std;

LazyScheduler::LazyScheduler(SimpleQueue *input_queue, 
                             SimpleQueue **feedback_queues, 
                             SimpleQueue **worker_queues, int num_workers, 
                             int cpu_number, cc_params::TableInit *params, 
                             int num_params, int max_chain, bool is_tpcc)
    : Runnable(cpu_number) {
    m_tables = do_tbl_init<Heuristic>(params, num_params);
    assert(m_tables != NULL);
    
    m_max_chain = max_chain;
    m_last_used = 0;
    m_is_tpcc = is_tpcc;
    
    m_num_stickified = 0;
    m_num_workers = num_workers;
    m_input_queue = input_queue;
    m_feedback_queues = feedback_queues;
    m_worker_queues = worker_queues;

    m_materialize_counter = 0;
    m_materialize_on = true;

    assert(m_input_queue != NULL);
    //    assert(m_feedback_queues != NULL);
    assert(m_worker_queues != NULL);
    
    // If we're running tpcc, pipeline the execution of the now phase. 
    if (is_tpcc) {
        char *internal_queue_values = (char*)malloc(LARGE_QUEUE*CACHE_LINE);
        memset(internal_queue_values, 0, LARGE_QUEUE);
        m_internal_queue = new SimpleQueue(internal_queue_values, LARGE_QUEUE);
        m_internal_flag = 0;
        pthread_create(&m_pipeline_thread, NULL, Pipeline, this);
        while (m_internal_flag == 0)
            ;
    }
}

void
LazyScheduler::StartWorking() {
    Action *txn;
    assert(m_input_queue != NULL);
    while (true) {        
        // Check if any of the workers need to continue a txn
        for (int i = 0; i < m_num_workers; ++i) {
            while (m_feedback_queues[i]->Dequeue((uint64_t*)&txn)) {
                if (txn->NowPhase()) {
                    if (m_is_tpcc) {
                        m_internal_queue->EnqueueBlocking((uint64_t)txn);
                    }
                    else {
                        AddGraph(txn);
                    }
                }
            }
        }

        // Check if there's any input
        if (m_input_queue->Dequeue((uint64_t*)&txn)) {
            //            txn->start_rdtsc_time = rdtsc();
            m_num_stickified += 1;
            if (txn->NowPhase()) {
                if (m_is_tpcc) {
                    m_internal_queue->EnqueueBlocking((uint64_t)txn);
                }
                else {
                    AddGraph(txn);
                }
            }
            //            txn->end_rdtsc_time = rdtsc();
        }        
    }
}

void*
LazyScheduler::Pipeline(void *arg) {
    LazyScheduler *me = (LazyScheduler*)arg;
    
    // Pin the thread to a cpu
    if (pin_thread(1) == -1) {
        std::cout << "Couldn't bind to a cpu!\n";
        exit(-1);
    }    
    xchgq(&me->m_internal_flag, 1);
    
    while (true) {
        Action *action;
        action = (Action*)me->m_internal_queue->DequeueBlocking();
        me->AddGraph(action);
    }
}

// Add the given action to the dependency graph. 
void LazyScheduler::AddGraph(Action* action) {
    action->state = STICKY;
        
    // Iterate through this action's read set and write set, find the 
    // transactions it depends on and add it to the dependency graph. 
    int num_reads = action->readset.size();
    int num_writes = action->writeset.size();
    int* count_ptrs[num_reads+num_writes];
        
    // Go through the read set. 
    uint32_t force_materialize = action->materialize;
    for (int i = 0; i < num_reads; ++i) {
        CompositeKey record = action->readset[i].record;
        Heuristic *dep_info = m_tables[record.m_table]->GetPtr(record.m_key);

        // Keep the information about the previous txn around.
        action->readset[i].dependency = dep_info->last_txn;	
        //        action->readset[i].dependency = NULL;
        action->readset[i].is_write = dep_info->is_write;
        action->readset[i].index = dep_info->index;
        count_ptrs[i] = &(dep_info->chain_length);
            
        // Update the heuristic information. 
        dep_info->last_txn = action;
        dep_info->index = i;
        dep_info->is_write = false;
            
        // Increment the length of the chain corresponding to this record, 
        // check if it exceeds our threshold value. 
        dep_info->chain_length += 1;
        force_materialize |= (uint32_t)(dep_info->chain_length >= m_max_chain);
    }
        
    // Go through the write set. 
    for (int i = 0; i < num_writes; ++i) {
        CompositeKey record = action->writeset[i].record;
        Heuristic *dep_info = m_tables[record.m_table]->GetPtr(record.m_key);
        
        // Keep the information about the previous txn around. 
        action->writeset[i].dependency = dep_info->last_txn;
        //        action->writeset[i].dependency = NULL;
        action->writeset[i].is_write = dep_info->is_write;
        action->writeset[i].index = dep_info->index;
        count_ptrs[num_reads+i] = &(dep_info->chain_length);
            
        // Update the heuristic information. 
        dep_info->last_txn = action;
        dep_info->index = i;
        dep_info->is_write = true;
            
        // Increment the length of the chain corresponding to this record, 
        // check if it exceeds our threshold value. 
        dep_info->chain_length += 1;
        force_materialize |= (uint32_t)(dep_info->chain_length >= m_max_chain);
    }  
    m_materialize_counter += 1;
    m_materialize_on |= m_materialize_counter & (1<<14);

    if (m_materialize_on && force_materialize) {
        int index = m_last_used % m_num_workers;
        //        clock_gettime(CLOCK_REALTIME, &action->start_time);
        if (action->is_blind) {
            action->state = PROCESSING;
        }
        if (!m_worker_queues[index]->Enqueue((uint64_t)action)) {
            m_materialize_counter = 0;
            m_materialize_on = false;
        }
        else {
            for (int i = 0; i < num_reads+num_writes; ++i) {
                *(count_ptrs[i]) = 0;		  
            }
        }
        m_last_used += 1;
    }
    /*
    else {
        action->state = STICKY;
        m_worker_queues[0]->EnqueueBlocking((uint64_t)action);
    }
    */
}


uint64_t
LazyScheduler::NumStickified() {
    return m_num_stickified;
}
