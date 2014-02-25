// Author: Jose Faleiro (faleiro.jose.manuel@gmail.com)
// 

#ifndef EXPERIMENT_INFO_H_
#define EXPERIMENT_INFO_H_

#include <getopt.h>
#include <stdlib.h>
#include <set>
#include <iostream>
#include <string>
#include <sstream>

#define NUM_OPTS 15

enum ExperimentType {
    THROUGHPUT,
    LATENCY,
    PEAK_LOAD,
    TPCC,
};

// Use this class to parse command line arguments for our particular experiment
// scenario. 
class ExperimentInfo {    

    void argError(struct option* long_options, int count) {
        std::cout << "lazy_db expects the following args:\n";
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
            {"normal", required_argument, NULL, 8},
            {"experiment", required_argument, NULL, 9},
            {"blind_writes", required_argument, NULL, 10},
            {"warehouses", required_argument, NULL, 11},
            {"districts", required_argument, NULL, 12},
            {"customers", required_argument, NULL, 13},
            {"items", required_argument, NULL, 14},
            { NULL, no_argument, NULL, 15}
        };
        
        warehouses = -1;
        districts = -1;
        customers = -1;
        items = -1;
        
        given_split = false;

        serial = true;
        substantiate_period = 1;
        is_normal = false;
        std_dev = -1;

        std::set<int> args_received;
        std::stringstream stick_stream;
        std::stringstream subst_stream;
        
        stick_stream << "eager";
        subst_stream << "eager";

        int exp_type = -1;
        blind_write_frequency = -1;
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
                serial = false;
                
                substantiate_period = atoi(optarg);
                stick_stream.str("");
                subst_stream.str("");

                subst_stream << "lazy_" << substantiate_period << "_subst";
                stick_stream << "lazy_" << substantiate_period << "_stick";
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
                is_normal = true;
                std_dev = atoi(optarg);
                break;
            case 9:
                exp_type = atoi(optarg);
                break;
            case 10:
                blind_write_frequency = atoi(optarg);
                break;
            case 11:
                warehouses = atoi(optarg);
                break;
            case 12:
                districts = atoi(optarg);
                break;
            case 13:
                customers = atoi(optarg);
                break;
            case 14:
                items = atoi(optarg);
                break;
            default:
                argError(long_options, NUM_OPTS);
            }
        }
        
        for (int i = 0; i < NUM_OPTS; ++i) {
            if (i != 10 && i != 8 && i != 0 && i != 10 && i != 11 && i != 12 &&
                i != 13 && i != 14) {
                if (args_received.find(i) == args_received.end()) {
                    std::cout << "Missing argument: ";
                    std::cout << long_options[i].name << "\n";
                    argError(long_options, NUM_OPTS);
                }
            }
        }
        
        if (exp_type != LATENCY && 
            exp_type != THROUGHPUT && 
            exp_type != PEAK_LOAD &&
            exp_type != TPCC) {
            argError(long_options, NUM_OPTS);
        }
        else {
            experiment = (ExperimentType)exp_type;
        }
        
        if ((exp_type == TPCC) && 
            (warehouses == -1 || districts == -1 || customers == -1 || 
             items == -1)) {
            argError(long_options, NUM_OPTS);
        }

        // Allocate cpu_set_t's for binding threads. 
        // XXX: The scheduler is single threaded so we have just one for now. 
        worker_bindings = new cpu_set_t[num_workers];
        scheduler_bindings = new cpu_set_t[2];		
        
        if (!serial) {
            subst_stream << "_threshold_" << substantiate_threshold;
            stick_stream << "_threshold_" << substantiate_threshold;
        }

        if (is_normal) {
            subst_stream << "_normal_" << std_dev;
            stick_stream << "_normal_" << std_dev;
        }
        else {
            subst_stream << "_uniform";
            stick_stream << "_uniform";
        }
	
        if (blind_write_frequency != -1) {
            subst_stream << "_blind_" << blind_write_frequency;
            stick_stream << "_blind_" << blind_write_frequency;
        }
	
        subst_stream << ".txt";
        stick_stream << ".txt";

        subst_file = subst_stream.str();
        stick_file = stick_stream.str();        
        
        std::cout << "Experiment info:\n";
        std::cout << subst_file << "\n";
        std::cout << stick_file << "\n";
    }
    
    int blind_write_frequency;

    // Binding information for scheduler+worker threads. 
    cpu_set_t* worker_bindings;
    cpu_set_t* scheduler_bindings;

    // Period between txns that force substantiation. 'f' means that we have one
    // every 'f' txns
    bool serial; 
    int substantiate_period;
    
    // Number of worker threads. 
    int num_workers;
    
    // Type of experiment for the coordinator to setup. 
    ExperimentType experiment;

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
    
    // File in which to dump substantiation throughput. 
    std::string subst_file;
    
    // File in which to dump stickification throughput. 
    std::string stick_file;

    // Number of times to repeat our experiment. 
    int num_runs;
    
    // Should we generate a normal distribution?
    bool is_normal;
    
    // Standard deviation of our normal distribution. 
    int std_dev;
    
    // TPCC specific data
    int warehouses;
    int districts;
    int customers;
    int items;

    bool given_split;

    uint32_t new_order;
    uint32_t district;
    uint32_t stock_level;
    uint32_t delivery;
    uint32_t order_status;
};

#endif
