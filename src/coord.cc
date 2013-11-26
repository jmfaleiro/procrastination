#include "worker.h"
#include "lazy_scheduler.h"
#include "cpuinfo.h"
#include "concurrent_queue.h"
#include "action.h"
#include "normal_generator.h"
#include "uniform_generator.h"
#include "experiment_info.h"
#include "machine.h"
#include "client.h"

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
        gen->start_time = 0;
        gen->end_time = 0;
        input_queue->EnqueueBlocking((uint64_t)gen);

        if (gen->materialize) {
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
    int* records = (int*)numa_alloc_local(sizeof(int)*CACHE_LINE*num_records*4);
    for (int i = 0; i < 4*num_records; ++i) {
        records[i*CACHE_LINE] = 1;
    }

  for (int i = 0; i < num_workers; ++i) {
	
	// Binding information for the current worker. 
	int cpu_id = get_cpu(start_index + i, 1);
	CPU_ZERO(&bindings[i]);
	CPU_SET(cpu_id, &bindings[i]);
        	
	// Start the worker. 
	workers[i] = new Worker(1 << 10,
                                &bindings[i], 
                                records);
	workers[i]->startThread(&input_queue[i], &output_queue[i]);	
	workers[i]->startWorker();
  }
}

// Returns the time elapsed in processing a given workload. 
timespec wait(LazyScheduler* sched,
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
void write_answers(ExperimentInfo* info, timespec time_taken, int num_done) {
    ofstream output_file;
    output_file.open(info->output_file, ios::app | ios::out);
    output_file << time_taken.tv_sec << "." << time_taken.tv_nsec <<  "  " << num_done << "\n";
    output_file.close();
}

void write_client_latencies(int num_waits, WorkloadGenerator* gen) {
    uint64_t* times = (uint64_t*)numa_alloc_local(sizeof(uint64_t)*num_waits);
    memset(times, 0, sizeof(uint64_t)*num_waits);

    int total_actions = gen->numUsed();
    Action* action_set = gen->getActions();
    int j = 0;
    for (int i = 0; i < total_actions; ++i) {
        Action cur = action_set[i];
        if (cur.materialize) {
            times[j++] = cur.end_time - cur.start_time;
        }
    }
    
    std::sort(times, times+j);
    
    ofstream output_file;
    output_file.open("client_latencies.txt", ios::out);
    
    double cdf_y = 0.0;
    double diff = 1.0 / (double)j;
    for (int i = 0; i < j; ++i) {
        output_file << cdf_y << " " << times[i] << "\n";
        cdf_y += diff;
    }
    
    output_file.close();
}

void write_latencies(Worker* worker) {
                     
        
    ofstream output_file;
    output_file.open("latencies.txt", ios::out);
    
    int num_values;
    uint64_t* times = worker->getTimes(&num_values);
    num_values = num_values > 1000000? 1000000 : num_values;
	std::cout << num_values << "\n";
	std::sort(times, times + num_values);

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
               SimpleQueue** input,
               LazyScheduler** scheduler,
               Worker** worker,
               WorkloadGenerator** generator) {

    init_cpuinfo();
    numa_set_strict(1);
    cpu_set_t my_binding;
    CPU_ZERO(&my_binding);
    CPU_SET(3, &my_binding);

    SimpleQueue** worker_inputs = 
        (SimpleQueue**)malloc(info->num_workers*sizeof(SimpleQueue*));

    SimpleQueue** worker_outputs = 
        (SimpleQueue**)malloc(info->num_workers*sizeof(SimpleQueue*));

    // Data for input/output queues. 
    uint64_t sched_size = (1 << 10);
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
    *generator = gen;

    // Create and start worker threads. 
    Worker* workers[info->num_workers];
    initWorkers(workers,
                2,
                info->num_workers, 
                info->worker_bindings,
                worker_inputs,
                worker_outputs,
                info->num_records);
    
    // Create a scheduler thread. 
    CPU_ZERO(&info->scheduler_bindings[0]);
    CPU_SET(0, &info->scheduler_bindings[0]);
	
    CPU_ZERO(&info->scheduler_bindings[1]);
    CPU_SET(1, &info->scheduler_bindings[1]);
	
    *scheduler = new LazyScheduler(info->serial,
                                   info->num_workers, 
                                   info->num_records, 
                                   info->substantiate_threshold,
                                   worker_inputs,
                                   NULL,
                                   info->scheduler_bindings,
                                   scheduler_input,
                                   NULL);

    (*scheduler)->startThread();
    *worker = workers[0];
    *output = worker_outputs[0];
    *input = scheduler_input;
    return 0;
}


void run_experiment(ExperimentInfo* info) {
    SimpleQueue* scheduler_output;
    SimpleQueue* scheduler_input;
    WorkloadGenerator* gen;
    LazyScheduler* sched;
    Worker* worker;
    int num_waits = 
        initialize(info, 
                   &scheduler_output, 
                   &scheduler_input, 
                   &sched, 
                   &worker, 
                   &gen);
    
    if (info->latency) {    
        Client c(worker, 
                 sched, 
                 scheduler_input, 
                 scheduler_output, 
                 gen, 
                 info->num_txns);
        c.RunPeak();
    }
    else {
        std::cout << "Begin building dependencies\n";
        buildDependencies(gen, info->num_txns, scheduler_input);
        std::cout << "End building dependencies\n";
        timespec exec_time = wait(sched, scheduler_output);
        write_answers(info, exec_time, sched->numDone());
        write_latencies(worker);
    }
    exit(0);
}

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    run_experiment(info);
    return 0;
}
