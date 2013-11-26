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

public:
    Client(LazyScheduler* scheduler,
           SimpleQueue* lazy_input, 
           SimpleQueue* lazy_output, 
           WorkloadGenerator* generator,
           int num_txns) {
        m_sched = scheduler;
        m_lazy_input = lazy_input;
        m_lazy_output = lazy_output;
        m_gen = generator;
        m_num_runs = num_txns;
    }

    /*
    void RunPeak() {
        
        uint64_t phase1 = 0, phase2 = 0, phase3 = 0;
        uint64_t start_time = rdtsc();
        uint64_t phase1_time, phase2_time, phase3_time;
        while (true) {
            ++phase1;
            Action* action = m_gen->genNext();
            m_lazy_input->EnqueueBlocking((uint64_t)action);
            phase1_time = rdtsc();
            if (phase1_time - start_time >= (1996 * 1000000)) {
                break;
            }
        }

        while (true) {
            ++phase2;
            Action* action = m_gen->genNext();
            m_lazy_input->EnqueueBlocking((uint64_t)action);
            phase2_time = rdtsc();
            if (phase2_time - start_time >= (2*1996*1000000)) {
                break;
            }
        }
        
        while (true) {
            ++phase3;

            uint64_t cur_time = rdtsc();
            if (phase3_time - start_time >= (3*1996*1000000)) {
                break;
            }
        }
    }
    */

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
                latencies[j] = latencies[j] / (1996.0);
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
