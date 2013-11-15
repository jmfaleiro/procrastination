#include "lazy_scheduler.h"

#include <assert.h>
#include <deque>

using namespace std;

int
LazyScheduler::depIndex(Action* action, int record, bool* is_write) {    
  // Iterate through every element of the read set and check whether it contains
  // the given record. Since we're dealing with the read set, set is_write to 
  // false. 
  *is_write = false;	
  int num_reads = action->readset_size();

  for (int i = 0; i < num_reads; ++i) {
    if (action->readset(i) == record) {
      return i;
    }
  }
  
  // Now check the writeset, the loop *MUST* terminate with the return statement
  // otherwise we have an unnecessary dependency. 
  int num_writes = action->writeset_size();
  *is_write = true;
  for (int i = 0; i < num_writes; ++i) {
    if (action->writeset(i) == record) {
      return i;
    }
  }
  assert(false);
}


LazyScheduler::LazyScheduler(uint64_t num_records, 
                             int max_chain,
                             ConcurrentQueue* worker_input,
                             ConcurrentQueue* worker_output,
                             cpu_set_t* binding_info,
                             ConcurrentQueue* sched_input,
                             ConcurrentQueue* sched_output) {

    m_worker_input = worker_input;
    
  m_worker_output = worker_output;
  m_binding_info = binding_info;
  m_log_input = sched_input;
  m_log_output = sched_output;

  m_num_records = (int)num_records;
  m_num_waiting = 0;
  m_num_inflight = 0;

  m_num_txns = 0;

  m_max_chain = max_chain;
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
    }

    sched->m_times = new uint64_t[1000000];
    sched->m_times_ptr = 0;

    xchgq(&(sched->m_start_flag), 1);
    
    ConcurrentQueue* incoming_txns = sched->m_log_input;
    ConcurrentQueue* done_txns = sched->m_worker_output;

    Action* action = NULL;
    while (true) {        
        if (sched->m_run_flag) {
            
            /*
            if (incoming_txns->Pop(&action)) {
                sched->addGraph(action);
            }
            
            while (done_txns->Pop(&action)) {
                sched->finishTxn(action);
                (sched->m_times)[(sched->m_times_ptr)++] = 
                    action->end_time() - action->exec_start_time();
            }
            */


            // First see if we have a new transaction to process.             
            volatile struct queue_elem* new_input;
            if ((new_input = incoming_txns->Dequeue(false)) != NULL) {
                action = (Action*)(new_input->m_data);
                sched->addGraph(action);

                // Add new_input to the free list.
                new_input->m_next = sched->m_free_elems;
                sched->m_free_elems = new_input;
                new_input->m_data = 0;
            }
            
            volatile struct queue_elem* done_action;
            while ((done_action = done_txns->Dequeue(false)) != NULL) {
                action = (Action*)(done_action->m_data);
                sched->store->returnElem(done_action);
                sched->finishTxn(action);
            }

        }
    }
    return NULL;
}

// Add the given action to the dependency graph. 
void LazyScheduler::addGraph(Action* action) {
    action->set_state(STICKY);

    // Iterate through this action's read set and write set, find the 
    // transactions it depends on and add it to the dependency graph. 
    int num_reads = action->readset_size();
    int num_writes = action->writeset_size();

  // Go through the read set. 
    bool force_materialize = action->materialize();
  for (int i = 0; i < num_reads; ++i) {
    int record = action->readset(i);
    Action* last_txn = (*m_last_txns)[record].last_txn;
    action->add_read_deps((uint64_t)last_txn);
    (*m_last_txns)[record].last_txn = action;
    
    // Increment the length of the chain corresponding to this record, check if 
    // it exceeds our threshold value. 
    (*m_last_txns)[record].chain_length += 1;
    force_materialize |= (*m_last_txns)[record].chain_length > m_max_chain;
  }

  // Go through the write set. 
  for (int i = 0; i < num_writes; ++i) {
    int record = action->writeset(i);    
    Action* last_txn = (*m_last_txns)[record].last_txn;
    action->add_write_deps((uint64_t)last_txn);
    (*m_last_txns)[record].last_txn = action;

    // Increment the length of the chain corresponding to this record, check if 
    // it exceeds our threshold value. 
    (*m_last_txns)[record].chain_length += 1;
    force_materialize |= (*m_last_txns)[record].chain_length > m_max_chain;    
  }  

  if (action->materialize() || force_materialize) {
      action->set_start_time(rdtsc());
      substantiate(action);
  }
}

void LazyScheduler::processRead(Action* action, 
                                int readIndex, 
                                deque<Action*>* queue) {
    assert(action->state() == ANALYZING);
    assert(readIndex < action->readset_size());
    assert(readIndex < action->read_deps_size());

    int num_dependencies = 0;
    
    int record = action->readset(readIndex);
    Action* prev = (Action*)(action->read_deps(readIndex));

    while (prev != NULL) {
        Action* start = prev;
        bool is_write;
        int index = depIndex(prev, record, &is_write);

        if (prev->state() == SUBSTANTIATED) {
            prev = NULL;
        }
        else if (is_write) {
            assert(prev->writeset(index) == record);
            
            // Start processing the dependency. 
            if (prev->state() == STICKY) {
                prev->set_state(ANALYZING);
                queue->push_back(prev);
            }

            // Add a dependency. 
            num_dependencies += 1;
            prev->add_dependents((uint64_t)action);
            
            // We can end since this is a write. 
            prev = NULL;
        }
        else {
            assert(prev->readset(index) == record);
            prev = (Action*)(prev->read_deps(index));
        }
        assert(start != prev);
    }
    assert(num_dependencies == 0 || num_dependencies == 1);

    // Update the number of dependencies. 
    int count = action->num_dependencies();
    count += num_dependencies;
    action->set_num_dependencies(count);
}

// We break if prev is NULL, or if it is a write
void LazyScheduler::processWrite(Action* action, 
                                 int writeIndex,
                                 deque<Action*>* queue) {
    assert(action->state() == ANALYZING);
    assert(writeIndex < action->writeset_size());
    assert(writeIndex < action->write_deps_size());

    int new_dependencies = 0;
    
    // Get the record and txn this action is dependent on. 
    int record = action->writeset(writeIndex);
    Action* prev = (Action*)action->write_deps(writeIndex);
    bool read_done = false;

    while (prev != NULL) {
        if (prev->state() != SUBSTANTIATED) {
            if (prev->state() == STICKY) {
                prev->set_state(ANALYZING);
                queue->push_back(prev);
            }
            prev->add_dependents((uint64_t)action);
            new_dependencies += 1;
        }

        bool is_write = false;
        int index = depIndex(prev, record, &is_write);
        if (is_write) {
            prev = NULL;
        }
        else {
            prev = (Action*)(prev->read_deps(index));
        }
    }
    
    int cur_count = action->num_dependencies();
    action->set_num_dependencies(cur_count + new_dependencies);
}

void LazyScheduler::run_txn(Action* to_run) {
    assert(to_run->state() == ANALYZING);
    to_run->set_state(PROCESSING);
    volatile struct queue_elem* to_enq = store->getNew();
    to_enq->m_data = (uint64_t)to_run;
    m_worker_input->Enqueue(to_enq);
}

uint64_t LazyScheduler::substantiate(Action* start) {
    uint64_t size_closure = 0;
    
    assert(start->state() == STICKY);

    // Use the search_queue variable to keep track of the list of unprocessed
    // transactions. First start with the single root we've been asked to 
    // process.
    start->set_state(ANALYZING);  
    deque<Action*> to_analyze;
    to_analyze.push_front(start);
    
  // Iterate throught the search queue until there's nothing more to process 
  while (!to_analyze.empty()) {
      Action* action = to_analyze.front();
      to_analyze.pop_front();
      
      // Make sure we've never seen this transaction before. 
      assert(action->state() == ANALYZING);
      assert(!action->has_num_dependencies());
      action->set_num_dependencies(0);
      size_closure += 1;

    // For each read dependency, get to the last transaction
    // to write the record which corresponds to this dependency. 
    int num_reads = action->readset_size();
    int num_writes = action->writeset_size();
    for (int i = 0; i < num_reads; ++i) {
        processRead(action, i, &to_analyze);
    }
    for (int i = 0; i < num_writes; ++i) {
        processWrite(action, i, &to_analyze);
    }
    
    // Safe to run the action if it doesn't have any dependencies. 
    if (action->num_dependencies() == 0) {
        ++m_num_inflight;
        action->set_exec_start_time(rdtsc());
        run_txn(action);
    }
    else {
        ++m_num_waiting;
    }
  }
  start->set_closure(size_closure);
  return 0;
}

// Mark the dependents as having completed. 
void LazyScheduler::finishTxn(Action* action) {
    if (action->state() != PROCESSING) {
        std::cout << action->state() << "\n";
    }
    assert(action->state() == PROCESSING);
    assert(m_num_inflight > 0);
    
    // Do some bookkeeping. 
    action->set_state(SUBSTANTIATED); 
    action->set_end_time(rdtsc());
    m_num_inflight -= 1;

    // Each dependent now has one less dependency. 
    int num_deps = action->dependents_size();
    for (int i = 0; i < num_deps; ++i) {
        Action* dep = (Action*)action->dependents(i);        
        int old_value = dep->num_dependencies();
        dep->set_num_dependencies(old_value - 1);
        
        // Check if it's ready to run. 
        if (old_value == 1) {
            action->set_exec_start_time(rdtsc());
            run_txn(dep);
            m_num_waiting -= 1;
            m_num_inflight += 1;
        }
    }

    // If there are no more inflight txns, there can be no more waiters!
    assert((m_num_inflight != 0) || (m_num_waiting == 0));
    
    int num_reads = action->readset_size();
    int num_writes = action->writeset_size();
    for (int i = 0; i < num_reads; ++i) {
        int record = action->readset(i);        
        (*m_last_txns)[record].chain_length -= 1;
    }
    for (int i = 0; i < num_writes; ++i) {
        int record = action->writeset(i);        
        (*m_last_txns)[record].chain_length -= 1;
    }

    // If this was a forced materialization, communicate it with the client. x
    if (action->materialize()) {
        //        ++m_num_txns;
        //        std::cout << m_num_txns << "\n";
        
        // Grab an element from our free list of elements to use. 
        volatile struct queue_elem* to_use = m_free_elems;
        m_free_elems = m_free_elems->m_next;
        to_use->m_data = (uint64_t)action;
        to_use->m_next = NULL;

        m_log_output->Enqueue(to_use);
    }
}

void 
LazyScheduler::startScheduler() {
  xchgq(&m_run_flag, 1);
}

void 
LazyScheduler::stopScheduler() {
  xchgq(&m_run_flag, 0);
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
