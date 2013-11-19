#include "lazy_scheduler.h"

#include <assert.h>
#include <deque>

using namespace std;

LazyScheduler::LazyScheduler(int num_workers,
                             int num_records, 
                             int max_chain,
                             SimpleQueue** worker_inputs,
                             SimpleQueue** worker_outputs,
                             cpu_set_t* binding_info,
                             SimpleQueue* sched_input,
                             SimpleQueue* sched_output) {

    m_worker_input = worker_inputs;
    m_num_workers = num_workers;
    
  m_worker_output = worker_outputs;
  m_binding_info = binding_info;
  m_log_input = sched_input;
  m_log_output = sched_output;

  m_num_records = (int)num_records;
  m_num_waiting = 0;
  m_num_inflight = 0;

  m_num_txns = 0;
  
  m_max_chain = max_chain;
  m_last_used = 0;
}

int LazyScheduler::getTimes(uint64_t** ret) {
    *ret = m_times;
    return m_times_ptr;
}

void* LazyScheduler::schedulerFunction(void* arg) {
    LazyScheduler* sched = (LazyScheduler*)arg;
    pin_thread(sched->m_binding_info);
    
    struct Heuristic empty;
    empty.last_txn = NULL;
    empty.chain_length = 0;

    sched->store = new ElementStore(1000000);
    sched->m_last_txns = 
        new vector<struct Heuristic>(sched->m_num_records, empty);

    for (int i = 0; i < sched->m_num_records; ++i) {
        (*(sched->m_last_txns))[i].last_txn = NULL;
        (*(sched->m_last_txns))[i].chain_length = 0;
        (*(sched->m_last_txns))[i].index = 0;
        (*(sched->m_last_txns))[i].is_write = false;
    }

    sched->m_times = new uint64_t[1000000];
    sched->m_times_ptr = 0;

    sched->m_num_txns = 0;
    xchgq(&(sched->m_start_flag), 1);
    
    SimpleQueue* incoming_txns = sched->m_log_input;
        Action* action = NULL;
    while (true) {        
        if (sched->m_run_flag) {
            uint64_t to_process;
            if (incoming_txns->Dequeue(&to_process)) {
                action = (Action*)to_process;
                sched->addGraph(action);
            }
            else {
                xchgq(&(sched->m_start_flag), 0);
            }            
        }
    }
    return NULL;
}

// Add the given action to the dependency graph. 
void LazyScheduler::addGraph(Action* action) {
	action->state = STICKY;

    // Iterate through this action's read set and write set, find the 
    // transactions it depends on and add it to the dependency graph. 
    int num_reads = action->readset.size();
    int num_writes = action->writeset.size();
	int* count_ptrs[num_reads+num_writes];

    // Go through the read set. 
    bool force_materialize = action->materialize;
    for (int i = 0; i < num_reads; ++i) {
		int record = action->readset[i].record;

        // Keep the information about the previous txn around. 
		action->readset[i].dependency = (*m_last_txns)[record].last_txn;
		action->readset[i].is_write = (*m_last_txns)[record].is_write;
		action->readset[i].index = (*m_last_txns)[record].index;
		count_ptrs[i] = &((*m_last_txns)[record].chain_length);

        // Update the heuristic information. 
        (*m_last_txns)[record].last_txn = action;
        (*m_last_txns)[record].index = i;
        (*m_last_txns)[record].is_write = false;
        
        // Increment the length of the chain corresponding to this record, 
        // check if it exceeds our threshold value. 
		(*m_last_txns)[record].chain_length += 1;		
		force_materialize |= (*m_last_txns)[record].chain_length >= m_max_chain;        
  }

  // Go through the write set. 
  for (int i = 0; i < num_writes; ++i) {
	  int record = action->writeset[i].record;

	  // Keep the information about the previous txn around. 
	  action->writeset[i].dependency = (*m_last_txns)[record].last_txn;
	  action->writeset[i].is_write = (*m_last_txns)[record].is_write;
	  action->writeset[i].index = (*m_last_txns)[record].index;
	  count_ptrs[num_reads+i] = &((*m_last_txns)[record].chain_length);

	  // Update the heuristic information. 
	  (*m_last_txns)[record].last_txn = action;
	  (*m_last_txns)[record].index = i;
	  (*m_last_txns)[record].is_write = true;

	  // Increment the length of the chain corresponding to this record, check if 
	  // it exceeds our threshold value. 
	  (*m_last_txns)[record].chain_length += 1;
	  force_materialize |= (*m_last_txns)[record].chain_length >= m_max_chain;        
  }  
  
  if (force_materialize) {      
	  for (int i = 0; i < num_reads+num_writes; ++i) {
		  *(count_ptrs[i]) = 0;		  
	  }
      substantiate(action);
  }
}

void LazyScheduler::processRead(Action* action, 
                                int readIndex) {
     
    assert(action->state == STICKY);
    
    int record = action->readset[readIndex].record;
	Action* prev = action->readset[readIndex].dependency;
    int index = action->readset[readIndex].index;
    bool is_write = action->readset[readIndex].is_write;

    while (prev != NULL) {

        if (prev->state == SUBSTANTIATED) {
            prev = NULL;
        }
        else if (is_write) {
            
            // Start processing the dependency. 
            if (prev->state == STICKY) {
                substantiate(prev);
            }

            // We can end since this is a write. 
            prev = NULL;
        }
        else {
			prev = prev->readset[index].dependency;
			is_write = prev->readset[index].is_write;
			index = prev->readset[index].index;
        }
    }
}

// We break if prev is NULL, or if it is a write
void LazyScheduler::processWrite(Action* action, 
                                 int writeIndex) {
                                
    assert(action->state == STICKY);
    
    // Get the record and txn this action is dependent on. 
    int record = action->writeset[writeIndex].record;
    Action* prev = action->writeset[writeIndex].dependency;
    bool is_write = action->writeset[writeIndex].is_write;
    int index = action->writeset[writeIndex].index;

    while (prev != NULL) {
        if (prev->state != SUBSTANTIATED) {
            substantiate(prev);
        }

        if (is_write) {
            prev = NULL;
        }
        else {
            prev = prev->readset[index].dependency;
            is_write = prev->readset[index].is_write;
            index = prev->readset[index].index;
        }
    }
}

void LazyScheduler::cleanup_txns() {
    for (int i = 0; i < m_num_workers; ++i) {
        uint64_t done_ptr;
        while (m_worker_output[i]->Dequeue(&done_ptr)) {
            finishTxn((Action*)done_ptr);
        }
    }
}

void LazyScheduler::run_txn(Action* to_run) {
    assert(to_run->state == SUBSTANTIATED);
    bool done = m_worker_input[0]->Enqueue((uint64_t)to_run);
    assert(done);
    ++m_num_txns;
}

uint64_t LazyScheduler::substantiate(Action* start) {    
    assert(start->state == STICKY);
    int num_reads = start->readset.size();
    int num_writes = start->writeset.size();

    for (int i = 0; i < num_reads; ++i) {
        processRead(start, i);
    }
    for (int i = 0; i < num_writes; ++i) {
        processWrite(start, i);
    }
    start->state = SUBSTANTIATED;
    run_txn(start);
    return 0;
}


int LazyScheduler::numDone() {
    return m_num_txns;
}

// Mark the dependents as having completed. 
void LazyScheduler::finishTxn(Action* action) {
    assert(false);
}

void 
LazyScheduler::startScheduler() {
  xchgq(&m_run_flag, 1);
}

void 
LazyScheduler::stopScheduler() {
  xchgq(&m_run_flag, 0);
}

uint64_t
LazyScheduler::waitFinished() {
    while (m_start_flag == 1)
        ;
    return m_num_txns;
}

void
LazyScheduler::startThread() {
    stopScheduler();
    m_start_flag = 0;
    asm volatile("":::"memory");
    pthread_create(&m_scheduler_thread,
                   NULL,
                   schedulerFunction,
                   this);
    
    // Wait for the worker to signal that it's started safely. 
    while(m_start_flag == 0)
		;
	
}
