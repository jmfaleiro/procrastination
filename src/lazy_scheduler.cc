#include "lazy_scheduler.h"
#include "machine.h"

#include <assert.h>
#include <deque>

using namespace std;

LazyScheduler::LazyScheduler(bool serial, 
			     bool blind,
                             bool throughput, 
                             int num_workers,
                             int num_records, 
                             int max_chain,
                             SimpleQueue** worker_inputs,
                             SimpleQueue** worker_outputs,
                             cpu_set_t* binding_info,
                             SimpleQueue* sched_input,
                             SimpleQueue* sched_output) {
    m_throughput = throughput;
	
    m_serial = serial;
    m_worker_input = worker_inputs;
    m_num_workers = num_workers;
    
    m_worker_output = worker_outputs;
    m_binding_info = binding_info;
    m_log_input = sched_input;
    m_log_output = sched_output;
    
    m_num_records = (int)num_records;
    m_num_waiting = 0;
    m_num_inflight = 0;
    
    m_num_saved = 0;
    m_num_txns = 0;
    
    m_proc_blind = blind;

    m_max_chain = max_chain;
    m_last_used = 0;
    
    m_stick = 0;
}

void* LazyScheduler::graphWalkFunction(void* arg) {
	LazyScheduler* sched = (LazyScheduler*)arg;
	pin_thread(&sched->m_binding_info[1]);

	SimpleQueue* input_queue = sched->m_walking_queue;
	sched->m_to_use = 0;
	sched->m_action_queue = new ActionQueue(10000000);

	xchgq(&sched->m_walk_flag, 1);
	
	if (sched->m_proc_blind) {
	  if (sched->m_throughput) {
            while (true) {
                uint64_t ptr;
                if (input_queue->Dequeue(&ptr)) {
		  Action* to_proc = (Action*)ptr;
		  if (to_proc->is_blind) {
		    sched->processBlindWrite(to_proc);
		  }
		  else {
		    sched->substantiateCart(to_proc);
		  }
                }
                else if (sched->m_start_flag == 2) {
                    xchgq(&sched->m_walk_flag, 2);
                }
            }
	  }
	  else {
	    while (true) {
	      Action* to_proc = (Action*)input_queue->DequeueBlocking();
	      if (to_proc->is_blind) {
	
		sched->processBlindWrite(to_proc);
	
	      }
	      else {
		sched->substantiateCart(to_proc);
	      }
	    }
	  }
	}
	
        if (sched->m_throughput) {
            while (true) {
                uint64_t ptr;
                if (input_queue->Dequeue(&ptr)) {
		  Action* to_proc = (Action*)ptr;

		  to_proc->cpu = sched->m_to_use;
		  sched->substantiate(to_proc);
		  sched->m_to_use = 
		    (sched->m_to_use + 1) % (sched->m_num_workers);

                }
		else if (sched->m_start_flag == 2 && 
			 sched->m_num_inflight == 0) {
		  xchgq(&sched->m_walk_flag, 2);
		}

		for (int i = 0; i < sched->m_num_workers; ++i) {
		  uint64_t done_raw;
		  while (sched->m_worker_output[i]->Dequeue(&done_raw)) {
		    sched->finishTxn((Action*)done_raw);
		  }
		}
		//		std::cout << sched->m_num_txns << '\n';
            }
        }

        else {
            while (true) {
                Action* to_proc = (Action*)input_queue->DequeueBlocking();
		to_proc->cpu = sched->m_to_use;
		sched->substantiate(to_proc);
		sched->m_to_use = 
		  (sched->m_to_use + 1) % (sched->m_num_workers);
		
		for (int i = 0; i < sched->m_num_workers; ++i) {
		  uint64_t done_raw;
		  while (sched->m_worker_output[i]->Dequeue(&done_raw)) {
		    sched->finishTxn((Action*)done_raw);
		  }
		}
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
    
    xchgq(&(sched->m_start_flag), 1);
    
    SimpleQueue* incoming_txns = sched->m_log_input;
    Action* action = NULL;
    sched->start_time = rdtsc();
    if (sched->m_throughput) {
        while (true) {        
            if (sched->m_run_flag) {
                uint64_t to_process;
                if (incoming_txns->Dequeue(&to_process)) {
                    action = (Action*)to_process;
                    sched->addGraph(action);
                }
                else if (sched->m_start_flag != 2) {
                    xchgq(&(sched->m_start_flag), 2);
                    sched->end_time = rdtsc();
                }            
            }
        }
    }
    else {
        while (true) {
            Action* to_process = (Action*)incoming_txns->DequeueBlocking();
            sched->addGraph(to_process);
            sched->m_stick += 1;
        }
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
	action->cpu = 0;
        run_txn(action);
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
        
		/*
	if (action->is_blind) {
	  for (int i = 0; i < num_writes; ++i) {
	    processBlindWrite(action, i);
	  }
	  }*/

        if (force_materialize) {
            for (int i = 0; i < num_reads+num_writes; ++i) {
                *(count_ptrs[i]) = 0;		  
            }
            m_walking_queue->EnqueueBlocking((uint64_t)action);
        }
    }
}

void LazyScheduler::processRead(Action* action, 
                                int readIndex) {
		
     
    assert(action->state == STICKY);    

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

void LazyScheduler::overwriteTxn(Action* action) {
  assert(action->state == STICKY);
  
  int num_writes = action->writeset.size();
  for (int i = 0; i < num_writes; ++i) {
    Action* prev = action->writeset[i].dependency;
    if (prev != NULL && prev->state == STICKY) {
      overwriteTxn(prev);
    }
  }
  
  action->state = SUBSTANTIATED;
  m_num_saved += 1;
}

void LazyScheduler::processRealDeps(Action* action) {
  assert(action->state == STICKY);
  
  int num_writes = action->writeset.size();
  for (int i = 0; i < num_writes; ++i) {
    Action* prev = action->writeset[i].dependency;
    if (prev != NULL && prev->state == STICKY) {
      processRealDeps(prev);
    }
  }

  action->state = SUBSTANTIATED;
  run_txn(action);
}

void LazyScheduler::substantiateCart(Action* action) {
  Action* prev = action->writeset[0].dependency;
  
  if (prev != NULL && prev->state != SUBSTANTIATED) {
    //    if (prev->state == STICKY) {
      processRealDeps(prev);
      //    }
      //    prev->state = SUBSTANTIATED;
      //    prev = prev->writeset[0].dependency;
  }
  action->state = SUBSTANTIATED;
  run_txn(action);
}

void LazyScheduler::processBlindWrite(Action* action) {
  assert(action->state == STICKY);

  action->state = SUBSTANTIATED;
  run_txn(action);  

  Action* prev = action->writeset[0].dependency;  
  if (prev != NULL && prev->state != SUBSTANTIATED) {
    overwriteTxn(prev);
  }
  // Go through all the transactions in this session and mark them as
  // substantiated. We don't have to touch them. 
  /*
  while (prev != NULL && prev->state != SUBSTANTIATED) {
    
    // If it's still a sticky, we've saved a transaction from being processed. 
    //    if (prev->state == STICKY) {
      overwriteTxn(prev);
      //    }
    prev->state = SUBSTANTIATED;
    prev = prev->writeset[0].dependency;
  }
  */
}

uint64_t LazyScheduler::getSaved() {
  return m_num_saved;
}

// We break if prev is NULL, or if it is a write
void LazyScheduler::processWrite(Action* action, 
                                 int writeIndex) {

                                
    assert(action->state == PROCESSING);
    
    // Get the record and txn this action is dependent on. 
    Action* prev = action->writeset[writeIndex].dependency;
    bool is_write = action->writeset[writeIndex].is_write;
    int index = action->writeset[writeIndex].index;

    while (prev != NULL) {
        if (prev->state != SUBSTANTIATED) {
	  if (prev->state == STICKY) {
	    prev->cpu = action->cpu;
            substantiate(prev);
	  }
	  ActionItem* item = m_action_queue->checkout();
	  item->value = action;
	  item->next = prev->wakeups;
	  prev->wakeups = item;

	  action->wait_count += 1;
        }

        if (is_write) {
	  //	  prev->writeset[index].wakeup = action;
            prev = NULL;
        }
        else {
            prev = prev->readset[index].dependency;
            is_write = prev->readset[index].is_write;
            index = prev->readset[index].index;
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
  assert(to_run->state == PROCESSING && to_run->wait_count == 0);
  //  assert((to_run->state & PROCESSING) == PROCESSING);
  m_worker_input[to_run->cpu]->EnqueueBlocking((uint64_t)to_run);  
  ++m_num_txns;
  ++m_num_inflight;
}

uint64_t LazyScheduler::substantiate(Action* start) {    
  assert(start->state == STICKY);
  start->state = PROCESSING;
  start->wakeups = NULL;
  start->wait_count = 0;
  int num_reads = start->readset.size();
  int num_writes = start->writeset.size();

  for (int i = 0; i < num_reads; ++i) {
    processRead(start, i);
  }
  for (int i = 0; i < num_writes; ++i) {
    processWrite(start, i);
  }

  if (start->wait_count == 0) {
    run_txn(start);
  }

  return 0;
}

uint64_t LazyScheduler::numStick() {
    return m_stick;
}

int LazyScheduler::numDone() {
    return m_num_txns;
}

// Mark the dependents as having completed. 
void LazyScheduler::finishTxn(Action* action) {
    
    action->state = SUBSTANTIATED;
    --m_num_inflight;

    ActionItem* to_wake = action->wakeups;
    while (to_wake != NULL) {
      Action* dep = to_wake->value;
      dep->wait_count -= 1;
      if (dep->wait_count == 0) {
	run_txn(dep);
      }
      to_wake = to_wake->next;
    }
    
    to_wake = action->wakeups;
    while (to_wake != NULL) {
      ActionItem* temp = to_wake;
      to_wake = to_wake->next;
      m_action_queue->checkin(temp);
    }
    m_log_output->EnqueueBlocking((uint64_t)action);
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
    while (m_start_flag != 2)
        ;
    return m_num_txns;
}

uint64_t
LazyScheduler::waitSubstantiated() {
    while (m_walk_flag != 2) 
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
