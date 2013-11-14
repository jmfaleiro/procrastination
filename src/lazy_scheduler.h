#ifndef LAZY_SCHEDULER_H
#define LAZY_SCHEDULER_H

#include <set>
#include <vector>
#include <deque>
#include <pthread.h>
#include <numa.h>
#include <tr1/unordered_map>
#include <list>
#include <vector>

#include "atomic.h"

#include "action_int.pb.h"
#include "concurrent_queue.h"
#include "cpuinfo.h"
#include "util.h"

using namespace std;

class LazyScheduler {

    volatile uint64_t m_run_flag;
    volatile uint64_t m_start_flag;    
    
    pthread_t m_scheduler_thread;

    enum TxnState {
        STICKY = 0,
        ANALYZING = 1,
        PROCESSING = 2,
        SUBSTANTIATED = 3,
    };
    
    set<Action*>* m_starts; 
    
    tr1::unordered_map<Action*, list<Action*>*>* m_dependents;
    tr1::unordered_map<Action*, int>* m_to_resolve;
    

    int m_num_records;

    int m_num_waiting;
    int m_num_inflight;
    
    uint64_t* m_times;
    int m_times_ptr;
    
    cpu_set_t* m_binding_info;
    
    // Time in cycles elapsed in substantiation, and number of txns completed.
    uint64_t m_subst;
    uint64_t m_num_txns;


  // Free list of struct queue_elems to use to communicate with workers. 
  ElementStore *store;

  volatile struct queue_elem* m_free_elems;

  // Last txns to touch a given record.
  vector<Action*>* m_last_txns;

  // Queue of actions to process.
  AtomicQueue<Action*>* m_worker_input;

  // Queue of completed actions. 
  AtomicQueue<Action*>* m_worker_output;
  
  // Queue from log.
  AtomicQueue<Action*>* m_log_input;

  // Output queue for scheduer. 
  AtomicQueue<Action*>* m_log_output;
  
  virtual void processWrite(Action* action, int index, deque<Action*>* queue);
  virtual void processRead(Action* action, int index, deque<Action*>* queue);

  virtual int depIndex(Action* action, int record, bool* is_write);
  
  // Force a transaction to substantiate. 
  virtual uint64_t substantiate(Action* action);

  // Clean up after a transaction is done.
  virtual void finishTxn(Action* action);

  // Scheduler thread function.
  static void* schedulerFunction(void* arg);
  
public:
  LazyScheduler(uint64_t num_records, 
                AtomicQueue<Action*>* input,
                AtomicQueue<Action*>* output,
                cpu_set_t* binding,
                AtomicQueue<Action*>* sched_input,
                AtomicQueue<Action*>* sched_output);
  
  // Add the action to the dependency graph. We add dependencies on the 
  // actions it depends on, and also update the m_last_txns table. 
  virtual void addGraph(Action* action);

  // Start processing elements from the input queue. 
  virtual void startScheduler();
  
  // Stop processing elements from the input queue. 
  virtual void stopScheduler();
  
  // Start the scheduler thread. 
  virtual void startThread();  
  
  virtual int getTimes(uint64_t** ret);
};

#endif // LAZY_SCHEDULER_H
