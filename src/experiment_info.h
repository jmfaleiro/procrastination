// Author: Jose Faleiro (faleiro.jose.manuel@gmail.com)
// 

#include <getopt.h>
#include <stdlib.h>
#include <set>
#include <iostream>

#define NUM_OPTS 10

// Use this class to parse command line arguments for our particular experiment
// scenario. 
class ExperimentInfo {    

    void argError(struct option* long_options, int count) {
        std::cout << "lazy_db expects the following args (all are required):\n";
        for (int i = 0; i < count; ++i) {
            std::cout << "\t" << long_options[i].name << "\n";
        }
        exit(0);
    }

public:
    ExperimentInfo(int argc, char** argv) {

        struct option long_options[] = {
            {"period", required_argument, NULL, 0},
            {"num_workers", required_argument, NULL, 1},
            {"num_reads", required_argument, NULL, 2},
            {"num_writes", required_argument, NULL, 3},
            {"num_records", required_argument, NULL, 4},
            {"num_txns", required_argument, NULL, 5},
            {"sub_threshold", required_argument, NULL, 6},
            {"num_runs", required_argument, NULL, 7},
            {"output_file", required_argument, NULL, 8},
            {"normal", required_argument, NULL, 9},
            { NULL, no_argument, NULL, 10}
        };
        
        is_normal = false;
        std_dev = -1;

        std::set<int> args_received;
        int index;
        while (getopt_long_only(argc, argv, "", long_options, &index) != -1) {
            
            // Make sure that we only ever get a single argument for a 
            // particular option. 
            if (args_received.find(index) != args_received.end()) {
                argError(long_options, NUM_OPTS);
            }
            else {
                args_received.insert(index);
            }
            switch (index) {
            case 0:
                substantiate_period = atoi(optarg);
                break;
            case 1:
                num_workers = atoi(optarg);
                break;
            case 2:
                read_set_size = atoi(optarg);
                break;
            case 3:
                write_set_size = atoi(optarg);
                break;
            case 4:
                num_records = atoi(optarg);
                break;
            case 5:
                num_txns = atoi(optarg);
                break;
            case 6:
                substantiate_threshold = atoi(optarg);
                break;
            case 7:
                num_runs = atoi(optarg);
                break;
            case 8:
                output_file = optarg;
                break;
            case 9:
                is_normal = true;
                std_dev = atoi(optarg);
                break;
            default:
                argError(long_options, NUM_OPTS);
            }
        }
        
        for (int i = 0; i < NUM_OPTS; ++i) {
            if (i != 9) {
                if (args_received.find(i) == args_received.end()) {
                    argError(long_options, NUM_OPTS);
                }
            }
        }
        
        // Allocate cpu_set_t's for binding threads. 
        // XXX: The scheduler is single threaded so we have just one for now. 
        worker_bindings = new cpu_set_t[num_workers];
        scheduler_bindings = new cpu_set_t[1];
    }
    
    // Binding information for scheduler+worker threads. 
    cpu_set_t* worker_bindings;
    cpu_set_t* scheduler_bindings;

    // Period between txns that force substantiation. 'f' means that we have one
    // every 'f' txns. 
    int substantiate_period;
    
    // Number of worker threads. 
    int num_workers;


    // Number of elements in the read set. 
    int read_set_size;
    
    // Number of elements in the write set. 
    int write_set_size;
    
    // The total number of records under consideration. 
    int num_records;
    
    // Number of txns to evaluate for this experiment.
    int num_txns;
    
    // Max length of txn chain before we force a substantiation. Need this for
    // "steady state" behavior. 
    int substantiate_threshold;
    
    // File in which to dump results. 
    // XXX: We're only going to dump throughput for now.. 
    char* output_file;

    // Number of times to repeat our experiment. 
    int num_runs;
    
    // Should we generate a normal distribution?
    bool is_normal;
    
    // Standard deviation of our normal distribution. 
    int std_dev;
};
