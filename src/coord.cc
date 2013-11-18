#include "worker.h"
#include "lazy_scheduler.h"
#include "cpuinfo.h"
#include "concurrent_queue.h"
#include "action_int.pb.h"
#include "normal_generator.h"
#include "uniform_generator.h"
#include "experiment_info.h"
#include "machine.h"

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
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
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
                  SimpleQueue* input_queue) {

    int to_wait = 0;
    for (int i = 0; i < num_actions; ++i) {
        Action* gen = generator->genNext();
        input_queue->EnqueueBlocking((uint64_t)gen);

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
            SimpleQueue** input_queue,
            SimpleQueue** output_queue,
            int num_records) {
    numa_set_strict(1);
    int* records = (int*)numa_alloc_local(sizeof(int)*CACHE_LINE*num_records);
    for (int i = 0; i < num_records; ++i) {
        records[i*CACHE_LINE] = 1;
    }

  for (int i = 0; i < num_workers; ++i) {
	
	// Binding information for the current worker. 
	int cpu_id = get_cpu(start_index + i, 1);
	CPU_ZERO(&bindings[i]);
	CPU_SET(cpu_id, &bindings[i]);
	
	// Start the worker. 
	workers[i] = new Worker(1 << 24,
                                &bindings[i], 
                                records);
	workers[i]->startThread(&input_queue[i], &output_queue[i]);	
	workers[i]->startWorker();
  }
}

// Returns the time elapsed in processing a given workload. 
timespec wait(int num_waits, 
              LazyScheduler* sched,
              SimpleQueue* scheduler_output) {
         
    timespec start_time, end_time;
    sched->startScheduler();
    
    // Start measuring time elapsed. 
    clock_gettime(CLOCK_REALTIME, &start_time);    
    
    uint64_t to_wait = sched->waitFinished();
    while (to_wait-- > 0) {
        volatile uint64_t ptr;
        ptr = scheduler_output->DequeueBlocking();
    }    
    clock_gettime(CLOCK_REALTIME, &end_time);    
    return diff_time(start_time, end_time);
}

// Append the timing information to an output file. 
void write_answers(ExperimentInfo* info, timespec time_taken) {
    ofstream output_file;
    output_file.open(info->output_file, ios::app | ios::out);
    output_file << time_taken.tv_sec << "." << time_taken.tv_nsec << "\n";
    output_file.close();
}

void write_latencies(Worker* worker) {
    ofstream output_file;
    output_file.open("latencies.txt", ios::out);
    
    int num_values;
    uint64_t* times = worker->getTimes(&num_values);
    num_values = num_values > 1000000? 1000000 : num_values;
    sort(times, times + num_values);

    double diff = 1.0 / (double)(num_values);
    double cur = 0.0;

    for (int i = 0; i < num_values; ++i) {
        output_file << cur << " " << times[i] << "\n";
        cur += diff;
    }
    output_file.close();
}

int initialize(ExperimentInfo* info, 
               SimpleQueue** output,
               LazyScheduler** scheduler,
               Worker** worker) {

    init_cpuinfo();
    numa_set_strict(1);
    cpu_set_t my_binding;
    CPU_ZERO(&my_binding);
    CPU_SET(2, &my_binding);

    SimpleQueue** worker_inputs = 
        (SimpleQueue**)malloc(info->num_workers*sizeof(SimpleQueue*));

    SimpleQueue** worker_outputs = 
        (SimpleQueue**)malloc(info->num_workers*sizeof(SimpleQueue*));

    // Data for input/output queues. 
    uint64_t sched_size = (1 << 24);
    uint64_t* sched_input_data = 
        (uint64_t*)malloc(CACHE_LINE*sizeof(uint64_t)*sched_size);
    uint64_t* sched_output_data = 
        (uint64_t*)malloc(CACHE_LINE*sizeof(uint64_t)*sched_size);

    assert(sched_input_data != NULL);
    assert(sched_output_data != NULL);

    // Create the queues. 
    SimpleQueue* scheduler_input = 
        new SimpleQueue(sched_input_data, sched_size);
    
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
        buildDependencies(gen, info->num_txns, scheduler_input);

    // Create and start worker threads. 
    Worker* workers[info->num_workers];
    initWorkers(workers,
                1,
                info->num_workers, 
                info->worker_bindings,
                worker_inputs,
                worker_outputs,
                info->num_records);
    
    // Create a scheduler thread. 
    *scheduler = new LazyScheduler(info->num_workers, 
                                   info->num_records, 
                                   info->substantiate_threshold,
                                   worker_inputs,
                                   NULL,
                                   &info->scheduler_bindings[0],
                                   scheduler_input,
                                   NULL);

    (*scheduler)->startThread();
    *worker = workers[0];
    *output = worker_outputs[0];
    return waits;
}


void run_experiment(ExperimentInfo* info) {
    SimpleQueue* scheduler_output;
    LazyScheduler* sched;
    Worker* worker;
    int num_waits = 
        initialize(info, &scheduler_output, &sched, &worker);
    timespec exec_time = wait(num_waits, sched, scheduler_output);
    std::cout << sched->numDone() << "\n";
    write_answers(info, exec_time);
    write_latencies(worker);
}

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    run_experiment(info);
    return 0;
}
