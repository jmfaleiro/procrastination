#include "worker.h"
#include "lazy_scheduler.h"
#include "cpuinfo.h"
#include "concurrent_queue.h"
#include "action_int.pb.h"
#include "normal_generator.h"
#include "uniform_generator.h"
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
                  ConcurrentQueue* input_queue,
                  ElementStore* store) {
    int to_wait = 0;
    for (int i = 0; i < num_actions; ++i) {
        Action* gen = generator->genNext();
        volatile struct queue_elem* to_use = store->getNew();
        to_use->m_data = (uint64_t)gen;
        input_queue->Enqueue(to_use);

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
            ConcurrentQueue* input_queue,
            ConcurrentQueue* output_queue,
            int num_records) {
    
    int *records = new int[num_records];
    for (int i = 0; i < num_records; ++i) {
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
              ConcurrentQueue* scheduler_output,
              ElementStore* store) {
    timespec start_time, end_time;
    sched->startScheduler();
    
    // Start measuring time elapsed. 
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);    

    // Wait for all non-lazy transactions to finish. 
    Action* done_txn;
    while (num_waits-- > 0) {

        // Wait for the scheduler to finish processing a txn. 
        volatile struct queue_elem* recv = scheduler_output->Dequeue(true);
        store->returnElem(recv);        
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
               ConcurrentQueue** output,
               LazyScheduler** scheduler,
               ElementStore** store) {

    init_cpuinfo();
    cpu_set_t my_binding;
    CPU_ZERO(&my_binding);
    CPU_SET(2, &my_binding);
    *store = new ElementStore(2000000);

    // Initialize queues for threads to talk to each other.
    // For worker <==> scheduler interactions. 
    ConcurrentQueue* worker_input = new ConcurrentQueue((*store)->getNew());
    ConcurrentQueue* worker_output = new ConcurrentQueue((*store)->getNew());
    
    // For scheduler <==> client interactions.
    ConcurrentQueue* scheduler_input = new ConcurrentQueue((*store)->getNew());
    ConcurrentQueue* scheduler_output = new ConcurrentQueue((*store)->getNew());
    
    // Create a workload generator and generate txns to process. 
    WorkloadGenerator* gen;
    if (info->is_normal) {
        gen = new NormalGenerator(info->read_set_size,
                                  info->write_set_size,
                                  info->num_records,
                                  info->substantiate_period,
                                  info->std_dev);
    }
    else {
        gen = new UniformGenerator(info->read_set_size,
                                   info->write_set_size,
                                   info->num_records,
                                   info->substantiate_period);
    }
    int waits = 
        buildDependencies(gen, info->num_txns, scheduler_input, *store);

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
                                   info->substantiate_threshold,
                                   worker_input,
                                   worker_output,
                                   &info->scheduler_bindings[0],
                                   scheduler_input,
                                   scheduler_output);
    (*scheduler)->startThread();
    
    *output = scheduler_output;
    return waits;
}


void run_experiment(ExperimentInfo* info) {
    ConcurrentQueue* scheduler_output;
    LazyScheduler* sched;
    ElementStore* store;
    int num_waits = initialize(info, &scheduler_output, &sched, &store);
    timespec exec_time = wait(num_waits, sched, scheduler_output, store);
    write_answers(info, exec_time);
}

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    run_experiment(info);
    return 0;
}
