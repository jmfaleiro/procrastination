#include "worker.h"
#include "lazy_scheduler.h"
#include "cpuinfo.h"
#include "concurrent_queue.h"
#include "action_int.pb.h"
#include "workload_generator.h"
#include "experiment_info.h"

#include <numa.h>
#include <pthread.h>
#include <sched.h>

#include "atomic.h"
#include <iostream> 
#include <fstream>
#include <time.h>
#include <algorithm>
#include <sstream>
#include <string>

std::vector<Action*>* last_txn_map = NULL;
std::vector<int>* value_map = NULL;
using namespace std;

timespec diff_time(timespec start, timespec end) 
{
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    }
    else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}


int
buildDependencies(WorkloadGenerator* generator,
                  int num_actions,
                  AtomicQueue<Action*>* input_queue,
                  ElementStore* store) {
    int to_wait = 0;
    for (int i = 0; i < num_actions; ++i) {
        Action* gen = generator->genNext();

        input_queue->Push(gen);

        /*
        volatile struct queue_elem* to_use = store->getNew();
        to_use->m_data = (uint64_t)gen;
        input_queue->Enqueue(to_use);
        */
        if (gen->materialize()) {
            ++to_wait;
        }
    }
    return to_wait;
}

void
initWorkers(Worker** workers,
            int start_index, 
            int num_workers, 
            cpu_set_t* bindings,
            AtomicQueue<Action*>* input_queue,
            AtomicQueue<Action*>* output_queue,
            int num_records) {
    
    int *records = new int[num_records];
    for (uint64_t i = 0; i < num_records; ++i) {
        records[i] = 1;
    }

  for (int i = 0; i < num_workers; ++i) {
	
	// Binding information for the current worker. 
	int cpu_id = get_cpu(start_index + i, 1);
	CPU_ZERO(&bindings[i]);
	CPU_SET(cpu_id, &bindings[i]);
	
	// Start the worker. 
	workers[i] = new Worker(input_queue, 
                                output_queue, 
                                &bindings[i], 
                                records);
	workers[i]->startThread();	
	workers[i]->startWorker();
  }
}

// Returns the time elapsed in processing a given workload. 
timespec wait(int num_waits, 
              LazyScheduler* sched,
              AtomicQueue<Action*>* scheduler_output) {
    timespec start_time, end_time;
    sched->startScheduler();
    
    // Start measuring time elapsed. 
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);    

    // Wait for all non-lazy transactions to finish. 
    Action* done_txn;
    while (num_waits-- > 0) {
        
        // Wait for the scheduler to finish processing a txn. 
        while (!scheduler_output->Pop(&done_txn)) 
            do_pause();
        
    }
    
    // Measure the curren time and return the time elapsed.
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);        
    return diff_time(start_time, end_time);
}

// Append the timing information to an output file. 
void write_answers(ExperimentInfo* info, timespec time_taken) {
    ofstream output_file;
    output_file.open(info->output_file, ios::app | ios::out);
    output_file << time_taken.tv_sec << "." << time_taken.tv_nsec << "\n";
    output_file.close();
}


int initialize(ExperimentInfo* info, 
               AtomicQueue<Action*>** output,
               LazyScheduler** scheduler) {
    cpu_set_t my_binding;
    CPU_ZERO(&my_binding);
    CPU_SET(79, &my_binding);
    
    // Initialize queues for threads to talk to each other.
    // For worker <==> scheduler interactions. 
    AtomicQueue<Action*>* worker_input = new AtomicQueue<Action*>();
    AtomicQueue<Action*>* worker_output = new AtomicQueue<Action*>();
    
    // For scheduler <==> client interactions.
    AtomicQueue<Action*>* scheduler_input = new AtomicQueue<Action*>();
    AtomicQueue<Action*>* scheduler_output = new AtomicQueue<Action*>();    
    
    // Create a workload generator and generate txns to process. 
    WorkloadGenerator gen(info->read_set_size, 
                          info->write_set_size, 
                          info->num_records,
                          info->substantiate_period);
    buildDependencies(&gen, info->num_txns, scheduler_input, NULL);

    // Create and start worker threads. 
    Worker* workers[info->num_workers];
    initWorkers(workers,
                1,
                info->num_workers, 
                info->worker_bindings,
                worker_input,
                worker_output,
                info->num_records);
    
    // Create a scheduler thread. 
    *scheduler = new LazyScheduler(info->num_records, 
                               worker_input,
                               worker_output,
                               &info->scheduler_bindings[0],
                               scheduler_input,
                               scheduler_output);

    (*scheduler)->startThread();
}


void run_experiment(ExperimentInfo* info) {
    AtomicQueue<Action*>* scheduler_output;
    LazyScheduler* sched;
    int num_waits = initialize(info, &scheduler_output, &sched);
    timespec exec_time = wait(num_waits, sched, scheduler_output);
    write_answers(info, exec_time);
}

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    run_experiment(info);
    return 0;
}
