#ifndef CLIENT_H
#define CLIENT_H

#include "lazy_scheduler.h"
#include "workload_generator.h"
#include "concurrent_queue.h"
#include "util.h"


#include <algorithm>
#include <fstream>



// Use this for "peak" workloads. 
class Client {
    
    // Interface to the database. 
    LazyScheduler* m_sched;
    SimpleQueue* m_lazy_input;
    SimpleQueue* m_lazy_output;
    WorkloadGenerator* m_gen;
    int m_num_runs;
    volatile uint64_t reader_flag;
    Worker* m_worker;
    

    void* readWorker(void* arg) {
        Client* client = (Client*)arg;
        cpu_set_t read_binding;
        CPU_ZERO(&read_binding);
	CPU_SET(4, &read_binding);
        
        // We have 100 points per second. 
        uint64_t* buckets = (uint64_t*)numa_alloc_local(sizeof(uint64_t)*300);
        memset(buckets, 0, sizeof(uint64_t)*300);

        xchgq(&reader_flag, 1);
        uint64_t start_time = rdtsc();
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 100; ++j) {
                while (true) {
                    uint64_t dummy;
                    if (client->m_lazy_output->Dequeue(&dummy)) {
                        buckets[100*i + j] += 1;
                    }
                    uint64_t cur_time = rdtsc();
                    if (cur_time - start_time >= (FREQUENCY/100)) {
                        start_time = cur_time;
                        break;
                    }
                }
            }
        }
        return buckets;
    }

public:
    Client(Worker* worker, 
           LazyScheduler* scheduler,
           SimpleQueue* lazy_input, 
           SimpleQueue* lazy_output, 
           WorkloadGenerator* generator,
           int num_txns) {
        m_worker = worker;
        m_sched = scheduler;
        m_lazy_input = lazy_input;
        m_lazy_output = lazy_output;
        m_gen = generator;
        m_num_runs = num_txns;
    }

    void RunPeak() {        
        m_gen->preGen();                
        m_sched->startScheduler();

        volatile uint64_t start_time = rdtsc();
        volatile uint64_t phase_time; 

        uint64_t* buckets_in = 
            (uint64_t*)numa_alloc_local(sizeof(uint64_t)*300);
        memset(buckets_in, 0, sizeof(uint64_t)*300);

        uint64_t* buckets_out = 
            (uint64_t*)numa_alloc_local(sizeof(uint64_t)*300);
        memset(buckets_out, 0, sizeof(uint64_t)*300);
        
        uint64_t* buckets_done = 
            (uint64_t*)numa_alloc_local(sizeof(uint64_t)*300);
        memset(buckets_done, 0, sizeof(uint64_t)*300);
        
        uint64_t* buckets_stick = 
            (uint64_t*)numa_alloc_local(sizeof(uint64_t)*300);
        memset(buckets_stick, 0, sizeof(uint64_t)*300);
        
        for (int i = 0; i < 8000000; ++i) {
            Action* action = m_gen->getPreGen();
            m_lazy_input->EnqueueBlocking((uint64_t)action);
            for (int i = 0; i < 10000; ++i) {
                single_work();
            }            
        }

        volatile int start_count = m_worker->numDone();
        volatile uint64_t start_stick = m_sched->numStick();
        // Regular load for 1 second. 
        for (int j = 0; j < 100; ++j) {
            while (true) {
                buckets_in[j] += 1;
                Action* action = m_gen->getPreGen();
                if (m_lazy_input->Enqueue((uint64_t)action)) {
                    buckets_out[j] += 1;
                }
                else {
                    for (int i = 0; i < 110; ++i) {
                        single_work();
                    }
                }
                phase_time = rdtsc();
                
                // Check whether or not 1 second has elapsed. 
                // XXX: The constant here is specific to smorz!!!
                if (phase_time - start_time >= (FREQUENCY/100)) {
                    start_time = phase_time;
                    volatile int end_count = m_worker->numDone();     
                    buckets_done[j] = end_count - start_count;
                    start_count = end_count;
                    
                    volatile uint64_t end_stick = m_sched->numStick();
                    buckets_stick[j] = end_stick - start_stick;
                    start_stick = end_stick;
                    break;
                }
                for (int i = 0; i < 100000; ++i) {
                    single_work();
                }
            }
        }

        // Spike in load for 1 second. 
        for (int j = 0; j < 100; ++j) {
            while (true) {
                buckets_in[100+j] += 1;
                Action* action = m_gen->getPreGen();
                if (m_lazy_input->Enqueue((uint64_t)action)) {
                    buckets_out[100+j] += 1;
                }
                
                else {
                    for (int i = 0; i < 800; ++i) {
                        single_work();
                    }
                }
                

                phase_time = rdtsc();

                // Check whether or not 1 second has elapsed. 
                // XXX: The constant here is specific to smorz!!!
                if (phase_time - start_time >= (FREQUENCY/100)) {
                    start_time = phase_time;
                    volatile int end_count = m_worker->numDone();
                    buckets_done[100+j] = end_count - start_count;
                    start_count = end_count;

                    volatile uint64_t end_stick = m_sched->numStick();
                    buckets_stick[100+j] = end_stick - start_stick;
                    start_stick = end_stick;

                    break;
                }
                for (int i = 0; i < 750; ++i) {
                    single_work();
                }
            }
        }
        
        // Regular load for 1 second. 
        for (int j = 0; j < 100; ++j) {
            while (true) {
                buckets_in[200+j] += 1;
                Action* action = m_gen->getPreGen();
                if (m_lazy_input->Enqueue((uint64_t)action)) {
                    buckets_out[200+j] += 1;
                }
                else {
                    for (int i = 0; i < 110; ++i) {
                        single_work();
                    }
                }

                phase_time = rdtsc();

                // Check whether or not 1 second has elapsed. 
                // XXX: The constant here is specific to smorz!!!
                if (phase_time - start_time >= (FREQUENCY/100)) {
                    start_time = phase_time;
                    volatile int end_count = m_worker->numDone();
                    buckets_done[200+j] = end_count - start_count;
                    start_count = end_count;

                    volatile uint64_t end_stick = m_sched->numStick();
                    buckets_stick[200+j] = end_stick - start_stick;
                    start_stick = end_stick;


                    break;
                }
                for (int i = 0; i < 100000; ++i) {
                    single_work();
                }
            }
        }
        
        ofstream client_try;

        
        client_try.open("client_try.txt");
        double cur = 0.0;
        double diff = 1.0 / 100.0;
        for (int i = 0; i < 300; ++i) {
            uint64_t throughput = buckets_in[i]*100;
            client_try << cur << " " << throughput << "\n";
            cur += diff;
        }

        client_try.close();

        ofstream client_success;
        client_success.open("client_success.txt");
        cur = 0.0;
        for (int i = 0; i < 300; ++i) {
            double percentage = 
                ((double)buckets_out[i])/((double)buckets_in[i]);
            percentage = percentage*100.0;
            client_success << cur << " " << percentage << "\n";
            cur += diff;
        }
        client_success.close();
        
        ofstream throughput_file;
        throughput_file.open("client_throughput.txt");
        cur = 0.0;
        for (int i = 0; i < 300; ++i) {
            uint64_t throughput = buckets_done[i]*100;
            throughput_file << cur << " " << throughput << "\n";
            cur += diff;
        }
        throughput_file.close();

        ofstream stick_file;
        stick_file.open("client_stickification.txt");
        cur = 0.0;
        for (int i = 0; i < 300; ++i) {
            uint64_t stick = buckets_stick[i]*100;
            stick_file << cur << " " << stick << "\n";
            cur += diff;
        }
        stick_file.close();
    }

    void Run() {
        
        uint64_t num_completed = 0;
        uint64_t to_wait = 0;

        m_sched->startScheduler();

        // Generate a bunch of txns to run, give them each a "start" time. 
        for (int i = 0; i < m_num_runs; ++i) {            
            Action* gen = m_gen->genNext();
            if (gen->materialize) {
                to_wait += 1;
            }
            gen->start_time = rdtsc();
            m_lazy_input->EnqueueBlocking((uint64_t)gen);
            gen->end_time = rdtsc();

            uint64_t ptr;
            while (m_lazy_output->Dequeue(&ptr)) {
                Action* done_action = (Action*)ptr;
                if (done_action->materialize) {
                    ++num_completed;
                    done_action->end_time = rdtsc();
                }

            }            
            /*
            for (int j = 0; j < 100; ++j) {
                single_work();
            }
            */
        }
        
        while (num_completed < to_wait) {
            Action* done_action = (Action*)m_lazy_output->DequeueBlocking();
            if (done_action->materialize) {
                done_action->end_time = rdtsc();
                ++num_completed;
            }
        }
        
        double* latencies = 
            (double*)numa_alloc_local(sizeof(double)*m_num_runs);
        Action* gen_actions = m_gen->getActions();
        int j = 0;
        for (int i = 0; i < m_num_runs; ++i) {
            Action cur = gen_actions[i];
            if (gen_actions[i].materialize && 
                gen_actions[i].start_time != 0 &&
                gen_actions[i].end_time != 0) {
                latencies[j] = 
                    1.0*(gen_actions[i].end_time - gen_actions[i].start_time);
                latencies[j] = latencies[j] / (1.0*FREQUENCY/1000000);
                ++j;
            }
        }
        
        ofstream output_file;
        output_file.open("client_latencies.txt", ios::out);
        
        std::sort(latencies, latencies+j);
        double diff = 1.0 / (double)j;
        double cdf_y = 0.0;
        for (int i = 0; i < j; ++i) {
            output_file << cdf_y << " " << latencies[i] << "\n";
            cdf_y += diff;
        }

        output_file.close();

        
    }        
    
};

#endif // CLIENT_H
