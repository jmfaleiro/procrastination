// Author: Jose Faleiro (faleiro.jose.manuel@gmail.com)
// 

#include <getopt.h>
#include <stdlib.h>
#include <set>
#include <iostream>

#define NUM_OPTS 8

// Use this class to parse command line arguments for our particular experiment
// scenario. 
class ExperimentInfo {    

    // Period between txns that force substantiation. 'f' means that we have one
    // every 'f' txns. 
    int m_substantiate_period;
    
    // Number of worker threads. 
    int m_num_workers;

    // Number of elements in the read set. 
    int m_read_set_size;
    
    // Number of elements in the write set. 
    int m_write_set_size;
    
    // The total number of records under consideration. 
    int m_num_records;
    
    // Number of txns to evaluate for this experiment.
    int m_num_txns;
    
    // Max length of txn chain before we force a substantiation. Need this for
    // "steady state" behavior. 
    int m_substantiate_threshold;

    int m_num_runs;
    

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
            { NULL, no_argument, NULL, 8}
    };


        std::set<int> args_received;
        int opt, index;
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
                m_substantiate_period = atoi(optarg);
                break;
            case 1:
                m_num_workers = atoi(optarg);
                break;
            case 2:
                m_read_set_size = atoi(optarg);
                break;
            case 3:
                m_write_set_size = atoi(optarg);
                break;
            case 4:
                m_num_records = atoi(optarg);
                break;
            case 5:
                m_num_txns = atoi(optarg);
                break;
            case 6:
                m_substantiate_threshold = atoi(optarg);
                break;
            case 7:
                m_num_runs = atoi(optarg);
                break;
            default:
                argError(long_options, NUM_OPTS);                
            }
        }
        
        for (int i = 0; i < NUM_OPTS; ++i) {
            if (args_received.find(i) == args_received.end()) {
                argError(long_options, NUM_OPTS);
            }
        }
    }
};
