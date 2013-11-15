#include "worker.h"

// For the microbenchmarks, read the values of the records in the read set and 
// then add the sum to each of the records in the writeset. 
void ProcessAction(const Action* to_proc, int* records) {
    int readset_size = to_proc->readset_size();
    int writeset_size = to_proc->writeset_size();

    // Sum all the values of the records in the read set. 
    int count = 0;
    for (int i = 0; i < readset_size; ++i) {
        int index = to_proc->readset(i);
        int value = records[index];
        count += value;
    }
    
    // Update the value of each of the records in the write-set. 
    for (int i = 0; i < writeset_size; ++i) {
        int index = to_proc->writeset(i);
        records[index] += count;
    } 
}

Worker::Worker(ConcurrentQueue* input, 
               ConcurrentQueue* output,
               cpu_set_t* binding_info,
               int* records) {
  m_input_queue = input;
  m_output_queue = output;
  m_binding_info = binding_info;
  m_records = records;
  m_run_flag = 0;
}

void* Worker::workerFunction(void* arg) {  
    
  Worker* worker = (Worker*)arg;
  pin_thread(worker->m_binding_info);

  // Signal that the thread is up and running. 
  xchgq(&(worker->m_start_signal), 1);
  
  ConcurrentQueue* input_queue = worker->m_input_queue;
  ConcurrentQueue* output_queue = worker->m_output_queue;

  uint64_t num_done = 0;
  Action* action;
  while (true) {
	if (worker->m_run_flag) {
            //            std::cout << "Worker thread is here!\n";
            ++num_done;
            volatile struct queue_elem* to_proc = input_queue->Dequeue(true);
            to_proc->m_next = NULL;

            action = (Action*)(to_proc->m_data);
            ProcessAction(action, worker->m_records);
            output_queue->Enqueue(to_proc);

            /*
            volatile struct queue_elem* to_use = input_queue->Dequeue(true);
            to_use->m_next = NULL;
            action = (const Action*)to_use->m_data;
            ProcessAction(action, worker->m_records);
            output_queue->Enqueue(to_use);
            */
	}
  }
  
  // We should never get here!
  assert(false);
}

void Worker::startWorker() {
  xchgq(&m_run_flag, 1);
}

void Worker::stopWorker() {
  xchgq(&m_run_flag, 0);
}

void Worker::waitForStart() {
  while (!m_start_signal)
	continue;
}

void Worker::startThread() {
  m_start_signal = 0;
  asm volatile("":::"memory");
  pthread_create(&m_worker_thread,
				 NULL,
				 workerFunction,
				 this);
  
  // Wait for the worker to signal that it's started safely. 
  while(m_start_signal == 0)
	;
}
