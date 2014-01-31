#ifndef WORKER_H
#define WORKER_H

#include "action.h"
#include "concurrent_queue.h"
#include "util.h"
#include "cpuinfo.h"
#include <pthread.h>
#include <numa.h>

class Worker {
  
  // Queues with which to communicate with the coordinator thread. 
  SimpleQueue* m_input_queue;
  SimpleQueue* m_output_queue;
  
  uint64_t* m_txn_latencies;
  volatile int m_num_values;

  int m_queue_size;

  // The "database" of records. 
  // XXX: Switch this to something more generic for later on. 
  int* m_records;
  
  uint64_t m_num_done;

  bool m_serial;
  uint64_t m_sched_done_flag;
  uint64_t m_done_flag;

  // Information used to bind to specific queues. 
  cpu_set_t* m_binding_info;  
  
  // Signal the worker thread to start trying to dequeue elements to process. 
  volatile uint64_t m_run_flag;
  
  // Worker signals this bit when the thread is up and running. 
  volatile uint64_t m_start_signal;
  
  pthread_t m_worker_thread;
  
  static void* workerFunction(void* arg);

  virtual void processRead(Action* action, int readIndex);
  virtual void processWrite(Action* action, int writeIndex);
  virtual void processBlindWrite(Action* action);
  virtual uint64_t substantiate(Action* action);

 public:
  Worker(int queue_size,
         cpu_set_t* binding_info,
         int* records,
	 bool serial);
  
  // Get the completion times for txns. 
  uint64_t* getTimes(int* num_vals);

  // Start processing elements from the input queue. 
  void startWorker();
  
  // Stop processing elements from the input queue. 
  void stopWorker();
  
  // Start the worker thread. 
  uint64_t* startThread(SimpleQueue** input, SimpleQueue** output);
  
  // Wait for the newly allocated thread to signal before returning. 
  void waitForStart();
  
  uint64_t getSaved();

  void waitSubstantiated();

  uint64_t numDone();
};

#endif // WORKER_H
