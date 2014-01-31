#include "lazy_scheduler.h"
#include "machine.h"

#include <assert.h>
#include <deque>

using namespace std;

LazyScheduler::LazyScheduler(bool serial, 
                             bool throughput, 
                             int num_workers,
                             int num_records, 
                             int max_chain,
			     uint64_t* worker_flag,
                             SimpleQueue** worker_inputs,
                             SimpleQueue** worker_outputs,
                             cpu_set_t* binding_info,
                             SimpleQueue* sched_input,
                             SimpleQueue* sched_output) {
    m_throughput = throughput;
	
    m_serial = serial;
    m_worker_input = worker_inputs;
    m_num_workers = num_workers;
    
    m_worker_flag = worker_flag;

    m_worker_output = worker_outputs;
    m_binding_info = binding_info;
    m_log_input = sched_input;
    m_log_output = sched_output;
    
    m_num_records = (int)num_records;
    m_num_waiting = 0;
    m_num_inflight = 0;
    
    m_num_saved = 0;
    m_num_txns = 0;
    
    m_max_chain = max_chain;
    m_last_used = 0;
    
    m_stick = 0;
}

void* LazyScheduler::graphWalkFunction(void* arg) {
  LazyScheduler* sched = (LazyScheduler*)arg;
  pin_thread(&sched->m_binding_info[1]);
  
  SimpleQueue* input_queue = sched->m_walking_queue;	
  xchgq(&sched->m_walk_flag, 1);	
  
  while (true) {
    Action* to_proc = (Action*)input_queue->DequeueBlocking();
     switch (to_proc->is_blind) {
    case 0:
      sched->substantiate(to_proc);
      break;
    case 1:
      to_proc->state = SUBSTANTIATED;
      sched->m_worker_input[0]->EnqueueBlocking((uint64_t)to_proc);
       sched->processBlindWrite(to_proc);
      break;
    }
  }
    return NULL;
}


void* LazyScheduler::schedulerFunction(void* arg) {
  numa_set_strict(1);
  LazyScheduler* sched = (LazyScheduler*)arg;
  pin_thread(&sched->m_binding_info[0]);
    
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

    sched->m_num_txns = 0;
    pthread_t walker_thread;
    
    /*
    if (!sched->m_serial) {
        
        // Create a queue to communicate with the graph walking thread. 
        int walk_queue_size = LARGE_QUEUE;
        uint64_t* walk_queue_data = 
            (uint64_t*)numa_alloc_local(CACHE_LINE*sizeof(uint64_t)*walk_queue_size);
        memset(walk_queue_data, 0, CACHE_LINE*sizeof(uint64_t)*walk_queue_size);
        sched->m_walking_queue = new SimpleQueue(walk_queue_data, walk_queue_size);
        
        // Kick off the graph walking thread. 
        xchgq(&sched->m_walk_flag, 0);
        pthread_create(&walker_thread,
                       NULL,
                       graphWalkFunction, 
                       sched);
        
        // Wait until the thread is ready..
        while (sched->m_walk_flag == 0)
            ;		
    }
    */

    xchgq(&(sched->m_start_flag), 1);
    
    SimpleQueue* incoming_txns = sched->m_log_input;
    Action* action = NULL;
    //    sched->start_time = rdtsc();
    while (true) {
      Action* to_process = (Action*)incoming_txns->DequeueBlocking();
      //to_process->sched_start_time = rdtsc();
      sched->addGraph(to_process); 
    }
 
    return NULL;
}

bool LazyScheduler::isDone(int* count) {
    if (m_start_flag == 0 && m_walking_queue->isEmpty()) {
        *count = m_num_txns;
        return true;
    }
    else {
        return false;
    }
}

// Add the given action to the dependency graph. 
void LazyScheduler::addGraph(Action* action) {
  if (m_serial) {
    action->state = SUBSTANTIATED;
    //    action->sched_end_time = rdtsc();    
    m_worker_input[0]->EnqueueBlocking((uint64_t)action);
    return;
  }    
    else {
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
            
            // Increment the length of the chain corresponding to this record, 
            // check if it exceeds our threshold value. 
            (*m_last_txns)[record].chain_length += 1;
            force_materialize |= (*m_last_txns)[record].chain_length >= m_max_chain;        
        }  

        if (force_materialize) {
            for (int i = 0; i < num_reads+num_writes; ++i) {
                *(count_ptrs[i]) = 0;		  
            }
            m_last_used += 1;
            
            int index = m_last_used % m_num_workers;
            m_worker_input[index]->EnqueueBlocking((uint64_t)action);
            //	    action->sched_end_time = rdtsc();
        }
    }
}

void LazyScheduler::processRead(Action* action, 
                                int readIndex) {
		
     
  //    assert(action->state == STICKY);    

    Action* prev = action->readset[readIndex].dependency;
    int index = action->readset[readIndex].index;
    bool is_write = action->readset[readIndex].is_write;
    
    while (prev != NULL) {
      if (is_write) {
            
	// Start processing the dependency. 
	if (prev->state == STICKY) {
	  substantiate(prev);
	}
	
	// We can end since this is a write. 
	prev = NULL;
      }
      else {
	int cur_index = index;
	is_write = prev->readset[cur_index].is_write;
	index = prev->readset[cur_index].index;
	prev = prev->readset[cur_index].dependency;
      }
    }
}


// XXX: In its current form, this function will only work when actions are 
// generated by the shopping cart workload generator. 

void LazyScheduler::processBlindWrite(Action* action) {

  // Hand over the blind-write to the worker thread. 
  //  action->state = SUBSTANTIATED;
  //  run_txn(action);  

  // Mark the transactions that made up the session as having been 
  // substantiated.
  Action* prev = action->writeset[0].dependency;  
  while (prev != NULL && prev->state != SUBSTANTIATED) {
    prev->state = SUBSTANTIATED;
    prev = prev->writeset[0].dependency;
    m_num_saved += 1;
  }
}

uint64_t LazyScheduler::getSaved() {
  return m_num_saved;
}

// We break if prev is NULL, or if it is a write
void LazyScheduler::processWrite(Action* action, 
                                 int writeIndex) {

                                
  //    assert(action->state == STICKY);
    
    // Get the record and txn this action is dependent on. 
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
	  int cur_index = index;
	  is_write = prev->readset[cur_index].is_write;
	  index = prev->readset[cur_index].index;
	  prev = prev->readset[cur_index].dependency;
        }
    }
}

/*
void LazyScheduler::cleanup_txns() {
    for (int i = 0; i < m_num_workers; ++i) {
        uint64_t done_ptr;
        while (m_worker_output[i]->Dequeue(&done_ptr)) {
            finishTxn((Action*)done_ptr);
        }
    }
}
*/

void LazyScheduler::run_txn(Action* to_run) {
  //assert(to_run->state == SUBSTANTIATED);
  //  to_run->end_time = rdtsc();
    m_worker_input[0]->EnqueueBlocking((uint64_t)to_run);
    //    bool done = m_worker_input[0]->Enqueue((uint64_t)to_run);
    //    assert(done);
    ++m_num_txns;
}

uint64_t LazyScheduler::substantiate(Action* start) {    
  //  assert(start->state == STICKY);
  int num_reads = start->readset.size();
  int num_writes = start->writeset.size();
  for (int i = 0; i < num_reads; ++i) {
    processRead(start, i);
  }
  for (int i = 0; i < num_writes; ++i) {
    processWrite(start, i);
  }

  start->state = SUBSTANTIATED;
  m_worker_input[0]->EnqueueBlocking((uint64_t)start);
  //  start->end_time = rdtsc();

  return 0;
}

uint64_t LazyScheduler::numStick() {
    return m_stick;
}

int LazyScheduler::numDone() {
    return m_num_txns;
}

// Mark the dependents as having completed. 
/*
void LazyScheduler::finishTxn(Action* action) {
    assert(false);
}
*/

void 
LazyScheduler::startScheduler() {
  xchgq(&m_run_flag, 1);
}

void 
LazyScheduler::stopScheduler() {
  xchgq(&m_run_flag, 0);
}

void
LazyScheduler::waitFinished() {
  while (!m_log_input->isEmpty())
        ;    
}

void
LazyScheduler::waitSubstantiated() {
    while (m_walk_flag != 2) 
        ;    
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
