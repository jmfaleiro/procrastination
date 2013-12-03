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
    
    bool m_proc_blind;

    pthread_t m_scheduler_thread;
    
    set<Action*>* m_starts; 
    
    tr1::unordered_map<Action*, list<Action*>*>* m_dependents;
    tr1::unordered_map<Action*, int>* m_to_resolve;
    
    uint64_t start_time;
    uint64_t end_time;

    bool m_throughput;

    int m_num_records;

    int m_num_waiting;
    int m_num_inflight;
    
    uint64_t* m_times;
    int m_times_ptr;
    
    cpu_set_t* m_binding_info;
    

    uint64_t m_num_saved;

    // Time in cycles elapsed in substantiation, and number of txns completed.
    volatile uint64_t m_stick;
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
  
  virtual void processBlindWrite(Action* action);
  void processWrite(Action* action, int index);
  virtual void processRead(Action* action, int index);

  void overwriteTxn(Action* action);
  void processRealDeps(Action* action);
  
  // Force a transaction to substantiate. 
  virtual uint64_t substantiate(Action* action);

  // Clean up after a transaction is done.
  virtual void finishTxn(Action* action);
  
  int m_to_use;

  // Dispatch a transaction to a worker for processing. 
  virtual void run_txn(Action* action);

  virtual void substantiateCart(Action* action);

  // Scheduler thread function.
  static void* schedulerFunction(void* arg);
  static void* graphWalkFunction(void* arg);


  //virtual void cleanup_txns();
  
public:
  LazyScheduler(bool serial,
		bool blind,
                bool throughput, 
                int num_workers, 
                int num_records, 
                int max_chain,
                SimpleQueue** input,
                SimpleQueue** output,
                cpu_set_t* binding,
                SimpleQueue* sched_input,
                SimpleQueue* sched_output);
  
  virtual uint64_t getSaved();
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

  virtual bool isDone(int* count);
  
  virtual uint64_t waitSubstantiated();
  
  virtual uint64_t numStick();
};

#endif // LAZY_SCHEDULER_H
