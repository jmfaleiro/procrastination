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

#include "action.h"
#include "concurrent_queue.h"
#include "cpuinfo.h"
#include "util.h"

using namespace std;

struct Heuristic {
    Action* last_txn;
    int index;
    bool is_write;
    int chain_length;
};


enum TxnState {
    STICKY = 0,
    ANALYZING = 1,
    PROCESSING = 2,
    SUBSTANTIATED = 3,
};


class LazyScheduler {

    volatile uint64_t m_run_flag;
    volatile uint64_t m_start_flag;
	volatile uint64_t m_walk_flag;

    pthread_t m_scheduler_thread;
    
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
    
    int m_max_chain;

    int m_num_workers;
    int m_last_used;

	bool m_serial;

  // Free list of struct queue_elems to use to communicate with workers. 
  ElementStore *store;

  volatile struct queue_elem* m_free_elems;

  // Last txns to touch a given record.
  vector<struct Heuristic>* m_last_txns;

  // Queue of actions to process.
  SimpleQueue** m_worker_input;

  // Queue of completed actions. 
  SimpleQueue** m_worker_output;
  
  SimpleQueue* m_walking_queue;

  // Queue from log.
  SimpleQueue* m_log_input;

  // Output queue for scheduer. 
  SimpleQueue* m_log_output;
  
  virtual void processWrite(Action* action, int index);
  virtual void processRead(Action* action, int index);
  
  // Force a transaction to substantiate. 
  virtual uint64_t substantiate(Action* action);

  // Clean up after a transaction is done.
  //  virtual void finishTxn(Action* action);
  
  // Dispatch a transaction to a worker for processing. 
  virtual void run_txn(Action* action);

  // Scheduler thread function.
  static void* schedulerFunction(void* arg);
  static void* graphWalkFunction(void* arg);


  //virtual void cleanup_txns();
  
public:
  LazyScheduler(bool serial,
				int num_workers, 
                int num_records, 
                int max_chain,
                SimpleQueue** input,
                SimpleQueue** output,
                cpu_set_t* binding,
                SimpleQueue* sched_input,
                SimpleQueue* sched_output);
  
  virtual uint64_t waitFinished();
  
  // Add the action to the dependency graph. We add dependencies on the 
  // actions it depends on, and also update the m_last_txns table. 
  virtual void addGraph(Action* action);

  // Start processing elements from the input queue. 
  virtual void startScheduler();
  
  // Stop processing elements from the input queue. 
  virtual void stopScheduler();
  
  // Start the scheduler thread. 
  virtual void startThread();  

  virtual int numDone();

  virtual bool isDone(uint64_t* num_done);
};

#endif // LAZY_SCHEDULER_H
