#include "worker.h"
#include "machine.h"
#include "lazy_scheduler.h"




// For the microbenchmarks, read the values of the records in the read set and 
// then add the sum to each of the records in the writeset. 
void ProcessAction(Action* to_proc, int* records) {
  to_proc->start_time = rdtsc();
    int readset_size = to_proc->readset.size();
    int writeset_size = to_proc->writeset.size();

    // Sum all the values of the records in the read set. 
    int count = 1;

    for (int i = 0; i < readset_size; ++i) {
        int index = to_proc->readset[i].record;
		/*
		for (int j = 0; i < CACHE_LINE*(index+4); ++i) {
			count += records[j];
		}
		*/
		count += records[CACHE_LINE*index];        
		count += records[CACHE_LINE*(index+1)];
		count += records[CACHE_LINE*(index+2)];
		count += records[CACHE_LINE*(index+3)];        	
    }
    
    
    // Update the value of each of the records in the write-set. 
    for (int i = 0; i < writeset_size; ++i) {
      int index = to_proc->writeset[i].record;

	  /*
		for (int j = 0; i < CACHE_LINE*(index+4); ++i) {
			records[j] += count;
			//			count += records[i];
		}
	  */
	  
	  records[CACHE_LINE * index] += count;
	  records[CACHE_LINE * (index+1)] += count;
	  records[CACHE_LINE * (index+2)] += count;
	  records[CACHE_LINE * (index+3)] += count;      
      
    }   
    to_proc->end_time = rdtsc();
}

Worker::Worker(int queue_size,
               cpu_set_t* binding_info,
               int* records,
	       bool serial) {
    m_queue_size = queue_size;
    m_binding_info = binding_info;
    m_records = records;
    m_run_flag = 0;
    m_input_queue = NULL;
    m_output_queue = NULL;

    m_serial = serial;
    m_num_done = 0;

    m_done_flag = 0;
    m_sched_done_flag = 0;
}

void* Worker::workerFunction(void* arg) {  
    numa_set_strict(1);    
  Worker* worker = (Worker*)arg;
  pin_thread(worker->m_binding_info);

  // Initialize the input and output queues. 
  int size = worker->m_queue_size;
  uint64_t* input_queue_data = 
      (uint64_t*)numa_alloc_local(size*CACHE_LINE*sizeof(uint64_t));
  uint64_t* output_queue_data = 
      (uint64_t*)numa_alloc_local(LARGE_QUEUE*CACHE_LINE*sizeof(uint64_t));
  memset(input_queue_data, 0, size*CACHE_LINE*sizeof(uint64_t));
  memset(output_queue_data, 0, LARGE_QUEUE*CACHE_LINE*sizeof(uint64_t));

  assert(input_queue_data != NULL);
  assert(output_queue_data != NULL);

  worker->m_input_queue = new SimpleQueue(input_queue_data, size);
  worker->m_output_queue = new SimpleQueue(output_queue_data, LARGE_QUEUE);

  SimpleQueue* input_queue = worker->m_input_queue;
  SimpleQueue* output_queue = worker->m_output_queue;

  // Signal that the thread is up and running. 
  xchgq(&(worker->m_start_signal), 1);
  
  worker->m_txn_latencies = 
      (uint64_t*)numa_alloc_local(sizeof(uint64_t) * 1000000);
  assert(worker->m_txn_latencies != NULL);
  memset(worker->m_txn_latencies, 0, sizeof(uint64_t)*1000000);

  worker->m_num_values = 0;
  if (worker->m_serial) {
    while (true) {
      Action* action = (Action*)input_queue->DequeueBlocking();
      action->system_start_time += rdtsc();
      ProcessAction(action, worker->m_records);
      output_queue->EnqueueBlocking((uint64_t)action);
      action->system_end_time += rdtsc();
      worker->m_num_done += 1;
    }
  }
  else {
    while (true) {
      uint64_t ptr = input_queue->DequeueBlocking();
      Action* action = (Action*)ptr;
      action->system_start_time += rdtsc();
      switch(action->is_blind) {
      case 0:	// It's a regular materialization.
	worker->substantiate((Action*)ptr);
	break;
	
      case 1:	// It's a blind write. 
	worker->processBlindWrite(action);
	break;
      }
  }
  if (worker->m_run_flag) {
    
      
    }
  }
  
  // We should never get here!
  assert(false);
}

void Worker::waitSubstantiated() {
  while (!m_input_queue->isEmpty())
    ;
}

uint64_t Worker::numDone() {
  return m_num_done;
}

uint64_t* Worker::getTimes(int* num_vals) {
    *num_vals = m_num_values;
    return m_txn_latencies;
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

uint64_t* Worker::startThread(SimpleQueue** input, SimpleQueue** output) {
  m_start_signal = 0;
  asm volatile("":::"memory");
  pthread_create(&m_worker_thread,
                 NULL,
                 workerFunction,
                 this);
  
  // Wait for the worker to signal that it's started safely. 
  while(m_start_signal == 0)
	;
  
  assert(m_input_queue != NULL);
  assert(m_output_queue != NULL);
  *input = m_input_queue;
  *output = m_output_queue;
  return &m_sched_done_flag;
}

void Worker::processRead(Action* action, 
			 int readIndex) {		
     
  //    assert(action->state == STICKY); 
    Action* prev = action->readset[readIndex].dependency;
    int index = action->readset[readIndex].index;
    bool is_write = action->readset[readIndex].is_write;
    
    bool ret = false;
    
    while (prev != NULL) {
      if (is_write) {
        int state;
        
	// Start processing the dependency. 
        Action::Lock(action);
        state = Action::CheckState(action);
        Action::Unlock(action);
        
	if (state == STICKY) {
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
void Worker::processBlindWrite(Action* action) {
  
  // Execute the blind-write and return it to the user. 
  action->state = SUBSTANTIATED;
  ProcessAction(action, m_records);
  action->system_end_time = rdtsc();
  m_output_queue->EnqueueBlocking((uint64_t)action);
  m_num_done += 1;

  // Mark the transactions that made up the session as having been 
  // substantiated.
  Action* prev = action->writeset[0].dependency;  
  while (prev != NULL && prev->state != SUBSTANTIATED) {
    prev->state = SUBSTANTIATED;
    prev = prev->writeset[0].dependency;
    m_num_done += 1;
  }
}

uint64_t Worker::substantiate(Action* start) {    
  //  assert(start->state == STICKY);
  int num_reads = start->readset.size();
  int num_writes = start->writeset.size();
  bool do_wait = false;
  for (int i = 0; i < num_reads; ++i) {
    processRead(start, i);
  }
  for (int i = 0; i < num_writes; ++i) {
    processWrite(start, i);
  }
  
  // We either need to process the transaction or wait for it to be completed. 
  int txn_state;
  Action::Lock(start);
  txn_state = Action::CheckState(start);
  if (txn_state == STICKY) {
    Action::ChangeState(start, PROCESSING);
  }
  Action::Unlock(start);
  
  if (txn_state != STICKY) {	// Wait for someone to finish the txn. 
    while ((txn_state == Action::CheckState(start)) != SUBSTANTIATED) {
      ;
    }      
  }
  else { 	// This thread's job to run the txn. 
    ProcessAction(start, m_records);
    Action::Lock(start);
    Action::ChangeState(start, SUBSTANTIATED);
    Action::Unlock(start);
  }

  //  start->state = SUBSTANTIATED;
  //  ProcessAction(start, m_records);
  //  start->system_end_time = rdtsc();
  m_output_queue->EnqueueBlocking((uint64_t)start);
  m_num_done += 1;
  return 0;
}

// We break if prev is NULL, or if it is a write
void Worker::processWrite(Action* action, 
                          int writeIndex) {

                                
  //    assert(action->state == STICKY);
    
    // Get the record and txn this action is dependent on. 
    Action* prev = action->writeset[writeIndex].dependency;
    bool is_write = action->writeset[writeIndex].is_write;
    int index = action->writeset[writeIndex].index;

    while (prev != NULL) {
      
      // Check if we need to recurse. 
      Action::Lock(prev);      
      int cur_state = Action::CheckState(prev);
      Action::Unlock(prev);
      if (cur_state == STICKY) {
        substantiate(prev);
      }
      
      // Check if the current transaction is a reader or a writer and 
      // accordingly keep processing them. 
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

