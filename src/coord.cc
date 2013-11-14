#include "worker.h"
#include "lazy_scheduler.h"
#include "cpuinfo.h"
#include "concurrent_queue.h"
#include "action_int.pb.h"
#include "workload_generator.h"

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


void buildDependenciesManual(AtomicQueue<Action*>* input_queue, 
                             ElementStore* store,
                             int num_elems) {

    int repeated = -1;
    std::set<int> top_seen;
    Action* top_action = new Action();
    assert(!top_action->has_materialize());
    for (int i = 0; i < 10; ++i) {
        if (i == 0) {
            repeated = rand() % num_elems;
            top_seen.insert(repeated);
            top_action->add_writeset(repeated);
        }
        else {
            while (true) {
                int to_add = rand() % num_elems;
                if (top_seen.count(to_add) == 0) {
                    top_seen.insert(to_add);
                    top_action->add_readset(to_add);
                    break;
                }
            }

        }
    }

    for (int i = 0; i < 100; ++i) {
        Action* gen = new Action();
        std::set<int> seen;
        for (int i = 0; i < 10; ++i) {


            if (i == 0 && repeated == -1) {
                repeated = rand() % num_elems;
            }
            if (i == 0) {
                gen->add_readset(repeated);
            }
            else {
                while (true) {
                    int to_add = rand() % num_elems;
                    if (seen.count(to_add) == 0) {
                        seen.insert(to_add);
                        gen->add_readset(to_add);
                        break;
                    }
                }
            }
        }
        for (int i = 0; i < 10; ++i) {
            while (true) {
                int to_add = rand() % num_elems;
                if (seen.count(to_add) == 0) {
                    seen.insert(to_add);
                    gen->add_writeset(to_add);
                    break;
                }
            }
        }
        input_queue->Push(gen);
        //        volatile struct queue_elem* blah = store->getNew();
        //        blah->m_data = (uint64_t)gen;
        //        input_queue->Enqueue(blah);        

    }
    
    Action* action = new Action();
    action->add_writeset(repeated);
    //action->set_materialize(true);
    
    input_queue->Push(action);

    /*
    volatile struct queue_elem* to_add = store->getNew();
    to_add->m_data = (uint64_t)action;
    input_queue->Enqueue(to_add);    
    */
    Action* read_action = new Action();
    read_action->add_readset(repeated);
    read_action->set_materialize(true);
    
    input_queue->Push(read_action);
    
    /*
    volatile struct queue_elem* blah = store->getNew();
    blah->m_data = (uint64_t)read_action;
    input_queue->Enqueue(blah);
    */
}

/*
void
writeFile(Action* action, ofstream stream) {
    int num_reads = action->readset_size();
    int num_writes = action->writeset_size();
    
    stream << "reads:" << num_reads << "\n";
    for (int i = 0; i < num_reads; ++i) {
        stream << action->readset(i) << "\n";
    }
    
    stream << "writeset:" << num_writes << "\n";
    for (int i = 0; i < num_writes; ++i) {
        stream << action->writeset(i) << "\n";
    }
}

deque<Action*>*
readFile(string file_name) {
    deque<Action*>* ret = new deque<Action*>();
    ifstream myfile(file_name);
    int num_actions;
    myfile >> num_actions;
}
*/
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
wait(LazyScheduler* sched, 
     int num_txns, 
     AtomicQueue<Action*>* output_queue, 
     timespec start_time,
     int freq,
     int num_threads) {

    uint64_t materialized = 0;
    uint64_t time_total = 0;
    uint64_t time_exec = 0;
    Action* action;
    while (num_txns-- > 0) {
        /*
        volatile struct queue_elem* returned = output_queue->Dequeue(true);
        action = (Action*)(returned->m_data);
        */
        while (!output_queue->Pop(&action))
            ;
        materialized += action->closure();
        //        time_total += action->end_time() - action->start_time();
        //        time_exec += action->end_time() - action->exec_start_time();
        //        std::cout << num_txns << "\n";
    }
    timespec end_time;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
    timespec diff = diff_time(start_time, end_time);
    
    uint64_t* times;
    int num_times = sched->getTimes(&times);
    std::sort(times, times+num_times);

    std::cout << "Txns done: " << materialized << "\n";
    std::cout << "Time: " << diff.tv_sec << "." << diff.tv_nsec << "ns.\n";
    
    ofstream myfile; 
    stringstream ss;
    ss << freq << "_" << num_threads << ".txt";    
    string output_file = ss.str();
    myfile.open(output_file.c_str(), std::fstream::out);
    for (int i = 0; i < num_times; ++i) {
        myfile << i << " " << times[i] << "\n";
    }
    myfile.close();

    //    std::cout << "Txns done: " << materialized << "\n";
    //    std::cout << "Time elapsed: " << time_total << "\n";
    //    std::cout << "Time executed: " << time_exec << "\n";
    exit(-1);
}
/*
void
buildSimple(int num_actions, 
            AtomicQueue<Action*>* sched_input,
            ElementStore* store) {
    Action* action;
    for (int i = 0; i < num_actions; ++i) {
        action = new Action();
        action->add_readset(i);
        action->add_writeset(i+1);
        action->set_closure(num_actions);

        if (i == num_actions - 1) {
            action->set_materialize(true);
        }

        volatile struct queue_elem* cur = store->getNew();
        cur->m_data = (uint64_t)action;
        sched_input->Enqueue(cur);
    }
}
*/
            
/*
void
processTxns(int num_txns, 
            AtomicQueue<Action*>* worker_input, 
            AtomicQueue<Action*>* worker_output,
            ElementStore* store) {
	
    // XXX: For now, give the workers, an empty action. 
    // Let them just return immediately. 
    for (int i = 0; i < num_txns; ++i) {
        volatile struct queue_elem* to_process = store->getNew();
        to_process->m_data = (uint64_t)NULL;
        worker_input->Enqueue(to_process);
    }
	
    // Wait for all the queue elems to be processed. 
    for (int i = 0; i < num_txns; ++i) {
        worker_output->Dequeue(true);
    }
    std::cout << "Done!\n";
}
*/

void
initWorkers(int start_index, 
            int num_workers, 
            Worker** workers,
            cpu_set_t* bindings,
            AtomicQueue<Action*>* input_queue,
            AtomicQueue<Action*>* output_queue,
            uint64_t num_records) {
    
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
	workers[i] = new Worker(input_queue, output_queue, &bindings[i], records);
	workers[i]->startThread();	
	workers[i]->startWorker();
  }
}

Worker**
initialize(int freq,
           int num_workers,
           int num_reads, 
           int num_writes,           
           uint64_t num_records, 
           int num_actions) {

    cpu_set_t my_binding;
    CPU_ZERO(&my_binding);
    CPU_SET(79, &my_binding);

    pin_thread(&my_binding);

  cpu_set_t sched_binding;

  // First bind the scheduler thread properly. 
  CPU_ZERO(&sched_binding);
  CPU_SET(0, &sched_binding);

  ElementStore* store = new ElementStore(1000000);

  // Initialize queues for communication. 
  AtomicQueue<Action*>* input_queue = new AtomicQueue<Action*>();
  AtomicQueue<Action*>* output_queue = new AtomicQueue<Action*>();
  
  AtomicQueue<Action*>* sched_queue = new AtomicQueue<Action*>();
  AtomicQueue<Action*>* sched_output = new AtomicQueue<Action*>();


  WorkloadGenerator work_gen(num_reads, num_writes, num_records, freq);
  int num_waits = 1;

  num_waits = buildDependencies(&work_gen, 
                                num_actions, 
                                sched_queue,
                                store);
      

  //    buildDependenciesManual(sched_queue, store, num_records);

  // Number of worker threads for this particular experiment. 
  init_cpuinfo();

  
  // Kick-start the worker threads. 
  cpu_set_t bindings[num_workers];
  Worker** workers = new Worker*[num_workers];

  initWorkers(1, 
              num_workers, 
              workers, 
              bindings, 
              input_queue, 
              output_queue, 
              num_records);

  // Initialize dependency graph of txns to process. 
  LazyScheduler* sched = new LazyScheduler(num_records, 
                                           num_waits,
                                           input_queue,
                                           output_queue,
                                           &sched_binding,
                                           sched_queue,
                                           sched_output);

  sched->startThread();
  sched->startScheduler();
  
  
  
  //  processTxns(100000, input_queue, output_queue, &store);
  timespec start_time;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
  wait(sched, num_waits, sched_output, start_time, freq, num_workers);
  /*

  buildSimple(sched, num_actions, sched_queue, &store);
  volatile struct queue_elem* done = sched_output->BlockingDequeue();
  Action* done_action = (Action*)(done->m_data);
  uint64_t with_walk = done_action->end_time() - done_action->start_time();
  uint64_t without_walk = 
      done_action->end_time() - done_action->exec_start_time();
  std::cout << "Num txns: " << done_action->closure() << "\n";
  std::cout << "Long path: " << with_walk << "\n";
  std::cout << "Path: " << without_walk << "\n";
  */
  return workers;
}


int
main(int argc, char** argv) {
    int freq = atoi(argv[1]);
    int num_workers = atoi(argv[2]);

    initialize(freq, num_workers, 10, 10, 10000000, 100000);  
  exit(-1);
}
