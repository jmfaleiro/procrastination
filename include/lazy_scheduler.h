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

#include <table.hh>
#include <action.h>
#include "concurrent_queue.h"
#include "cpuinfo.h"
#include "util.h"

using namespace std;

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
    
    int m_binding_info;
    
    
    uint64_t* m_worker_flag;

    uint64_t m_num_saved;

    // Time in cycles elapsed in substantiation, and number of txns completed.
    volatile uint64_t m_stick;
    uint64_t m_num_txns;
    
    uint32_t m_table_size;
    int m_max_chain;

    int m_num_workers;
    int m_last_used;

	bool m_serial;


  // Last txns to touch a given record.
 Table<CompositeKey, struct Heuristic>  *m_last_txns;


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
  virtual void processWrite(Action* action, int index);
  virtual void processRead(Action* action, int index);

  void overwriteTxn(Action* action);
  void processRealDeps(Action* action);
  
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
                bool throughput, 
                int num_workers, 
                int num_records, 
                int max_chain,
                uint64_t* worker_flag,
                SimpleQueue** input,
                SimpleQueue** output,
                int binding,
                SimpleQueue* sched_input,
                SimpleQueue* sched_output);
  
  virtual uint64_t getSaved();
  virtual void waitFinished();
  
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
  
  virtual void waitSubstantiated();
  
  virtual uint64_t numStick();
};

#endif // LAZY_SCHEDULER_H
