#ifndef WORKER_H
#define WORKER_H

#include "action_int.pb.h"
#include "concurrent_queue.h"
#include "util.h"
#include "cpuinfo.h"
#include <pthread.h>
#include <numa.h>

class Worker {
  
  // Queues with which to communicate with the coordinator thread. 
  ConcurrentQueue* m_input_queue;
  ConcurrentQueue* m_output_queue;

  // The "database" of records. 
  // XXX: Switch this to something more generic for later on. 
  int* m_records;

  // Information used to bind to specific queues. 
  cpu_set_t* m_binding_info;  
  
  // Signal the worker thread to start trying to dequeue elements to process. 
  volatile uint64_t m_run_flag;
  
  // Worker signals this bit when the thread is up and running. 
  volatile uint64_t m_start_signal;
  
  pthread_t m_worker_thread;
  
  static void* workerFunction(void* arg);

 public:
  Worker(ConcurrentQueue* input, 
         ConcurrentQueue* output, 
         cpu_set_t* binding_info,
         int* records);
  
  // Start processing elements from the input queue. 
  void startWorker();
  
  // Stop processing elements from the input queue. 
  void stopWorker();
  
  // Start the worker thread. 
  void startThread();
  
  // Wait for the newly allocated thread to signal before returning. 
  void waitForStart();
};

#endif // WORKER_H
