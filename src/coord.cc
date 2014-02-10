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
#include "shopping_cart.h"
#include <tpcc.h>


#include <numa.h>
#include <pthread.h>
#include <sched.h>

#include <iostream> 
#include <fstream>
#include <time.h>
#include <algorithm>
#include <sstream>
#include <string>

std::vector<Action*>* last_txn_map = NULL;
std::vector<int>* value_map = NULL;
using namespace std;

static timespec diff_time(timespec start, timespec end) 
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

uint64_t*
initWorkers(Worker** workers,
            int start_index, 
            int num_workers, 
            cpu_set_t* bindings,
            SimpleQueue** input_queue,
            SimpleQueue** output_queue,
            int num_records,
            ExperimentInfo* info) {
    numa_set_strict(1);
    int* records = (int*)numa_alloc_local(sizeof(int)*CACHE_LINE*num_records*4);
    memset(records, 1, sizeof(int)*CACHE_LINE*num_records*4);

  for (int i = 0; i < num_workers; ++i) {
	
	// Binding information for the current worker. 
	int cpu_id = get_cpu(start_index + i, 1);
        	
	// Start the worker. 
        if (info->experiment == PEAK_LOAD) {
            workers[i] = new Worker(SMALL_QUEUE,
                                    cpu_id,
                                    records,
				    info->serial);
        }
        else {
            workers[i] = new Worker(LARGE_QUEUE,
                                    cpu_id,
                                    records,
				    info->serial);            
        }
	uint64_t* ret = NULL;
	ret = workers[i]->startThread(&input_queue[i], &output_queue[i]);	
        assert(input_queue[i] != NULL && output_queue[i] != NULL);
	workers[i]->startWorker();
	return ret;
  }
}

// Returns the time elapsed in processing a given workload. 
timespec wait(LazyScheduler* sched,
              SimpleQueue* scheduler_output, 
              Worker** workers,
              int num_workers,
              timespec* stickification_time,
              int* num_done,
              ExperimentInfo* info) {

    timespec start_time, end_time, input_time;
    sched->startScheduler();
    
    // Start measuring time elapsed. 
    clock_gettime(CLOCK_REALTIME, &start_time);   
    int to_wait = 0, done_count = 0;

    sched->waitFinished();	
    clock_gettime(CLOCK_REALTIME, &input_time);    
    workers[0]->waitSubstantiated();
    *num_done = workers[0]->numDone();

    /*
    else {
      to_wait = info->num_txns;
      while (done_count++ < to_wait) {
	scheduler_output->DequeueBlocking();
      }
      *num_done = info->num_txns;
      }*/
       
    clock_gettime(CLOCK_REALTIME, &end_time);    
    input_time = diff_time(start_time, input_time);
    stickification_time->tv_sec = input_time.tv_sec;
    stickification_time->tv_nsec = input_time.tv_nsec;
    return diff_time(start_time, end_time);
}

// Append the timing information to an output file. 
void write_answers(ExperimentInfo* info, 
                   timespec subst_time, 
                   timespec stick_time,
                   int num_done) {
    ofstream subst_file;
    subst_file.open(info->subst_file.c_str(), ios::app | ios::out);
    subst_file << subst_time.tv_sec << "." << subst_time.tv_nsec <<  "  " << num_done << "\n";
    subst_file.close();

    if (!info->serial) {
        ofstream stick_file;
        std::cout << stick_time.tv_sec << "." << stick_time.tv_nsec << "\n";
        stick_file.open(info->stick_file.c_str(), ios::app | ios::out);
        stick_file << stick_time.tv_sec << "." ;
        stick_file << stick_time.tv_nsec << " " << info->num_txns << "\n";
        stick_file.close();
    }
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

void write_client_latencies(WorkloadGenerator* gen) {
  vector<uint64_t> latencies;
  int action_count = gen->numUsed();
  Action* gen_actions = gen->getActions();
  for (int i = 0; i < action_count; ++i) {
    if (gen_actions[i].materialize) {
      latencies.push_back(gen_actions[i].system_end_time + 
			  gen_actions[i].sched_end_time -
			  gen_actions[i].sched_start_time -
			  gen_actions[i].system_start_time);
    }
  }
  
  ofstream output_file;
  output_file.open("system_latencies.txt");
  
  int num_values = latencies.size();
  std::sort(latencies.begin(), latencies.end());
  double diff = 1.0 / (double)(latencies.size());
  double cur = 0.0;
  
  for (int i = 0; i < num_values; ++i) {
    output_file << cur << " " << latencies[i] << "\n";
    cur += diff;
  }
  output_file.close();
}

void write_latencies(WorkloadGenerator* gen) {
  
  vector<uint64_t> latencies;
  int action_count = gen->numUsed();
  Action* gen_actions = gen->getActions();
  for (int i = 0; i < action_count; ++i) {
    if (gen_actions[i].start_time != 0 && gen_actions[i].end_time != 0) {
      latencies.push_back(gen_actions[i].end_time - gen_actions[i].start_time);
    }
  }
        
    ofstream output_file;
    output_file.open("txn_latencies.txt", ios::out);
    
    int num_values = latencies.size();
    std::sort(latencies.begin(), latencies.end());

    double diff = 1.0 / (double)(latencies.size());
    double cur = 0.0;

    for (int i = 0; i < num_values; ++i) {
      output_file << cur << " " << latencies[i] << "\n";
      cur += diff;
    }
    output_file.close();
}


void initialize(ExperimentInfo* info, 
                SimpleQueue** output,
                SimpleQueue** input,
                LazyScheduler** scheduler,
                Worker** workers,
                WorkloadGenerator** generator) {

    init_cpuinfo();
    numa_set_strict(1);
    cpu_set_t my_binding;
    CPU_ZERO(&my_binding);
    CPU_SET(9, &my_binding);

    SimpleQueue** worker_inputs = 
        (SimpleQueue**)malloc(info->num_workers*sizeof(SimpleQueue*));

    SimpleQueue** worker_outputs = 
        (SimpleQueue**)malloc(info->num_workers*sizeof(SimpleQueue*));
    memset(worker_inputs, 0, info->num_workers*sizeof(SimpleQueue*));
    memset(worker_inputs, 0, info->num_workers*sizeof(SimpleQueue*));

    // Data for input/output queues. 
    uint64_t sched_size;
    if (info->experiment == PEAK_LOAD) {
        sched_size = SMALL_QUEUE;
    }
    else {
        sched_size = LARGE_QUEUE;
    }

    uint64_t* sched_input_data = 
        (uint64_t*)malloc(CACHE_LINE*sizeof(uint64_t)*sched_size);
    memset(sched_input_data, 0, CACHE_LINE*sizeof(uint64_t)*sched_size);

    assert(sched_input_data != NULL);

    // Create the queues. 
    SimpleQueue* scheduler_input = 
        new SimpleQueue(sched_input_data, sched_size);

    // Create a workload generator and generate txns to process. 
    WorkloadGenerator* gen = NULL;
    if (info->blind_write_frequency != -1) {
      gen = new ShoppingCart(1000,
			     info->num_records, 
			     20,
			     info->blind_write_frequency,
			     10000);
    }
    else if (info->experiment == TPCC) {
        cout << "Initializing tpcc!\n";
        cout << "Num warehouses: " << info->warehouses << "\n";
        cout << "Num districts: " << info->districts << "\n";
        cout << "Num customers: " << info->customers << "\n";
        cout << "Num items: " << info->items << "\n";
        
        tpcc::TPCCInit tpcc_initializer(info->warehouses, info->districts, 
                                        info->customers, info->items);
        tpcc_initializer.do_init();

    }
    else if (info->is_normal) {
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
    if (info->experiment == THROUGHPUT) {
      buildDependencies(gen, info->num_txns, scheduler_input);    
    }
    // Create and start worker threads. 
    uint64_t* worker_flag = initWorkers(workers,
					1,
					info->num_workers, 
					info->worker_bindings,
					worker_inputs,
					worker_outputs,
					info->num_records,
					info);
    
    // Create a scheduler thread. 
    CPU_ZERO(&info->scheduler_bindings[0]);
    CPU_SET(0, &info->scheduler_bindings[0]);
	
    CPU_ZERO(&info->scheduler_bindings[1]);
    CPU_SET(1, &info->scheduler_bindings[1]);
	
    bool throughput_expt = (info->experiment == THROUGHPUT);
    *scheduler = new LazyScheduler(info->serial,
                                   throughput_expt,
                                   info->num_workers, 
                                   info->num_records, 
                                   info->substantiate_threshold,
                                   worker_flag,
                                   worker_inputs,
                                   NULL,
                                   0,
                                   scheduler_input,
                                   NULL);

    (*scheduler)->startThread();
    *output = worker_outputs[0];
    *input = scheduler_input;
}


void run_experiment(ExperimentInfo* info) {
    SimpleQueue* scheduler_output;
    SimpleQueue* scheduler_input;
    WorkloadGenerator* gen;
    LazyScheduler* sched;
    Worker** workers = (Worker**)malloc(sizeof(Worker*) * info->num_workers);   
    initialize(info, &scheduler_output, &scheduler_input, &sched, workers, &gen);

    if (info->experiment == THROUGHPUT) {
        int num_done;
        timespec input_time;
        timespec exec_time = wait(sched, 
                                  scheduler_output, 
                                  workers, 
                                  info->num_workers,
                                  &input_time, 
                                  &num_done,
                                  info);
        std::cout << "here!\n";
        write_answers(info, exec_time, input_time, num_done);
        write_latencies(gen);     
        write_client_latencies(gen);
    }
    else if (info->experiment == LATENCY) {
        Client c(*workers, 
                 sched, 
                 scheduler_input, 
                 scheduler_output, 
                 gen, 
                 info->num_txns);
        c.Run();
    }
    else if (info->experiment == PEAK_LOAD) {
        Client c(*workers, 
                 sched, 
                 scheduler_input, 
                 scheduler_output, 
                 gen, 
                 info->num_txns);
        c.RunPeak();
    }
    else if (info->experiment == TPCC) {
        
    }
    exit(0);
}

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    run_experiment(info);
    return 0;
}
