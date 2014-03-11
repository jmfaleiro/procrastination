#include <eager_experiment.hh>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>

using namespace std;

EagerExperiment::EagerExperiment(ExperimentInfo *info) 
    : Experiment(info) {
}

void
EagerExperiment::InitializeTPCCLockManager() {
    using namespace tpcc;
    using namespace cc_params;

    TableInit *lock_mgr_params = 
        (TableInit*)malloc(sizeof(TableInit)*s_num_tables);
    
    for (int i = 0; i < (int)s_num_tables; ++i) {
        switch (i) {            

        case WAREHOUSE:
            GetWarehouseTableInit(&lock_mgr_params[i]);
            break;
        case DISTRICT:
            GetDistrictTableInit(&lock_mgr_params[i]);
            break;
        case CUSTOMER:
            GetCustomerTableInit(&lock_mgr_params[i]);
            break;
        case HISTORY:
            GetHistoryTableInit(&lock_mgr_params[i]);
            break;
        case NEW_ORDER:
            GetNewOrderTableInit(1<<24, &lock_mgr_params[i], true);
            break;
        case OPEN_ORDER:
            GetOpenOrderTableInit(1<<24, &lock_mgr_params[i], true);
            break;            
        case ORDER_LINE:	// All order lines implicitly sync on open order
            GetEmptyTableInit(&lock_mgr_params[i]);
            break;
        case ITEM:			// The items table is static, avoid locking it
            GetEmptyTableInit(&lock_mgr_params[i]);
            break;
        case STOCK:
            GetStockTableInit(&lock_mgr_params[i]);
            break;
        case OPEN_ORDER_INDEX:
            GetOpenOrderIndexTableInit(&lock_mgr_params[i]);
            break;
        case NEXT_DELIVERY:
            GetNextDeliveryTableInit(&lock_mgr_params[i]);
            break;
        default:
            std::cout << "Got an unexpected tpcc table type!\n";
            exit(-1);
        }
    }
    m_lock_mgr = new LockManager(lock_mgr_params, s_num_tables);
}


void
EagerExperiment::InitInputs(SimpleQueue **input_queues, int num_inputs, int num_workers,
                            EagerGenerator *gen) {
    //    uint32_t no, pay, stock, 
    timespec zero_time;
    zero_time.tv_sec = 0;
    zero_time.tv_nsec = 0;
    
    m_actions = (EagerAction**)malloc(sizeof(EagerAction*)*num_inputs);
    for (int i = 0; i < num_inputs; ++i) {
        EagerAction *txn = gen->genNext();
        m_actions[i] = txn;
        m_actions[i]->start_time = zero_time;
        m_actions[i]->end_time = zero_time;
        m_actions[i]->start_rdtsc_time = 0;
        m_actions[i]->end_rdtsc_time = 0;        
        input_queues[i%num_workers]->EnqueueBlocking((uint64_t)txn);
    }
}

EagerWorker**
EagerExperiment::InitWorkers(int num_workers, SimpleQueue **input_queues, 
                             SimpleQueue **output_queues, int cpu_offset) {
    EagerWorker **ret = new EagerWorker*[num_workers];
    for (int i = 0; i < num_workers; ++i) {
        ret[i] = new EagerWorker(m_lock_mgr, input_queues[i], output_queues[i], 
                                 i+cpu_offset);
    }
    return ret;
}

void
EagerExperiment::DoThroughputExperiment(EagerWorker **workers, 
                                        SimpleQueue **output_queues, 
                                        int num_workers, uint32_t num_waits) {
    
    // Bind this thread to a particular cpu to avoid interfering with workers
    if (pin_thread(num_workers+1) == -1) {
        std::cout << "Eager experiment: Client thread couldn't bind to cpu!!!";
        std::cout << "\n";
        exit(-1);
    }
    
    // Start the workers
    for (int i = 0; i < num_workers; ++i) {
        workers[i]->Run();
    }
    
    timespec start_time, end_time;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);

    // Spin until the workers have finished processing their inputs
    uint32_t num_done = 0;
    while (num_done < num_waits) {
        for (int i = 0; i < num_workers; ++i) {
            uint64_t dummy;
            if (output_queues[i]->Dequeue(&dummy)) {
                ++num_done;
            }
        }
    }
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
    
    timespec diff = diff_time(end_time, start_time);
    WriteThroughput(diff, num_waits);
    std::cout << diff.tv_sec << "." << diff.tv_nsec << "\n";
}

void
EagerExperiment::WriteStockCDF() {
    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
        if (dynamic_cast<StockLevelEager0*>(m_actions[i]) != NULL) {
            StockLevelEager0 *level0 = (StockLevelEager0*)m_actions[i];
            EagerAction *level1, *level2;
            level0->IsLinked(&level1);
            level1->IsLinked(&level2);
            assert((level0->start_time.tv_sec != 0 || level0->start_time.tv_nsec != 0) &&
                   (level2->start_time.tv_sec != 0 || level2->start_time.tv_nsec != 0));                   
            timespec diff = diff_time(level2->end_time, level0->start_time);
            times[count++] = (1000000.0*diff.tv_sec) + (diff.tv_nsec/1000.0);
        }
    }
    stringstream no_file;
    no_file << "results/eager_" << m_info->warehouses << "_threads_" << m_info->num_workers << "_stock.txt";
    ofstream cdf_file;
    cdf_file.open(no_file.str(), ios::out);

    std::sort(&times[0], &times[count]);
    double fraction = 0.0;
    double diff = 1.0/(count*1.0);    
    for (int i = 0; i < count; ++i) {
        cdf_file << fraction << " " << times[i] << "\n";
        fraction  += diff;        
    }
    cdf_file.close();
}

void
EagerExperiment::WriteNewOrderCDF() {
    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
        if (dynamic_cast<NewOrderEager*>(m_actions[i]) != NULL) {
            assert(m_actions[i]->end_rdtsc_time != 0 && m_actions[i]->start_rdtsc_time != 0);
            times[count++] = (1.0*(m_actions[i]->end_rdtsc_time - m_actions[i]->start_rdtsc_time)) / 1996.0;
        }
    }

    stringstream no_file;
    no_file << "results/eager_" << m_info->warehouses << "_threads_" << m_info->num_workers << "_neworder.txt";
    ofstream cdf_file;
    cdf_file.open(no_file.str(), ios::out);

    std::sort(&times[0], &times[count]);
    double fraction = 0.0;
    double diff = 1.0/(count*1.0);    
    for (int i = 0; i < count; ++i) {
        cdf_file << fraction << " " << times[i] << "\n";
        fraction  += diff;        
    }
    cdf_file.close();
}


void
EagerExperiment::RunTPCC() {    
    InitializeTPCCLockManager();
    
    // These queues will hold input to each worker thread
    SimpleQueue **input_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **output_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    
    // Initialize the workload generator
    EagerTPCCGenerator txn_generator;
    if (m_info->given_split) {
        txn_generator = EagerTPCCGenerator(m_info->new_order, m_info->district, 
                                           m_info->stock_level, m_info->delivery,
                                           m_info->order_status);
    }
    else {
        txn_generator = EagerTPCCGenerator(45, 43, 5, 5, 5);
    }
    InitInputs(input_queues, m_info->num_txns, m_info->num_workers, &txn_generator);    
    EagerWorker **workers = InitWorkers(m_info->num_workers, input_queues, 
                                        output_queues, 0);

    
    DoThroughputExperiment(workers, output_queues, m_info->num_workers, 
                           (uint32_t)m_info->num_txns);    
    //    WriteNewOrderCDF();
    //    WriteStockCDF();
}

void
EagerExperiment::RunBlind() {
    TableInit table_init_params[2];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = 2000;

    table_init_params[1].m_table_type = ONE_DIM_TABLE;
    table_init_params[1].m_params.m_one_params.m_dim1 = 1000000;

    m_lock_mgr = new LockManager(table_init_params, 2);
    
    SimpleQueue **input_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **output_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    
    // Initialize the workload generator
    EagerGenerator *gen = new EagerShoppingCart(2000, 1000000, 20, m_info->blind_write_frequency, 0);
    EagerWorker **workers = InitWorkers(m_info->num_workers, input_queues, 
                                        output_queues, 0);

    InitInputs(input_queues, m_info->num_txns, m_info->num_workers, gen);
    DoThroughputExperiment(workers, output_queues, m_info->num_workers, 
                           (uint32_t)m_info->num_txns);    
    WriteBlindLatencies();    
}

void
EagerExperiment::RunPeak() {
    TableInit table_init_params[1];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = m_info->num_records;
    
    // Use a uniform distribution on keys
    EagerGenerator *gen = new EagerUniformGenerator(m_info->read_set_size, 
                                                    m_info->write_set_size, 
                                                    m_info->num_records, 
                                                    m_info->substantiate_period);

    // Generate the input actions
    EagerAction **input_actions = 
        (EagerAction**)malloc(sizeof(EagerAction*)*m_info->num_txns);
    for (int i = 0; i < m_info->num_txns; ++i) {
        input_actions[i] = gen->genNext();
    }

    // Create the lock manager
    m_lock_mgr = new LockManager(table_init_params, 1);    
    
    // Create the workers
    SimpleQueue **input_queues = InitQueues(m_info->num_workers, SMALL_QUEUE);
    SimpleQueue **output_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    EagerWorker **workers = InitWorkers(m_info->num_workers, input_queues, 
                                        output_queues, 1);
    m_workers = workers;

    // Create the scheduler
    SimpleQueue **sched_input = InitQueues(1, SMALL_QUEUE);
    EagerScheduler *sched = new EagerScheduler(m_lock_mgr, sched_input[0], 
                                               input_queues, (uint32_t)m_info->num_workers,
                                               0);
    sched->Run();
    for (int i = 0; i < m_info->num_workers; ++i) {
        workers[i]->Run();
    }

    // Do the experiment
    WaitPeak(180, input_actions, sched_input[0]);
}

uint32_t 
EagerExperiment::NumWorkerDone() {
    uint64_t ret = 0;
    for (int i = 0; i < m_info->num_workers; ++i) {
        ret += m_workers[i]->NumProcessed();
    }
    return ret;
}

void
EagerExperiment::WaitPeak(uint32_t duration,
                          EagerAction **input_actions, 
                          SimpleQueue *input_queue) {

    if (pin_thread(m_info->num_workers+1) == -1) {
        std::cout << "Eager experiment: Client thread couldn't bind to cpu!!!";
        std::cout << "\n";
        exit(-1);
    }

    volatile uint64_t start_time = rdtsc();
    volatile uint64_t phase_time; 
    duration *= 10;

    uint64_t* buckets_in = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_in, 0, sizeof(uint64_t)*(duration+2*duration/3));

    uint64_t* buckets_out = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_out, 0, sizeof(uint64_t)*(duration+2*duration/3));
        
    uint64_t* buckets_done = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_done, 0, sizeof(uint64_t)*(duration+2*duration/3));
        
    volatile uint64_t start_count = NumWorkerDone();
    
    // Run easy input for the first third of the experiment
    uint64_t input_index = 0;
    uint32_t j = 0;
    for (; j < duration/3; ++j) {
        while (true) {
            buckets_in[j] += 1;
            EagerAction *action = input_actions[input_index++];
            if (input_queue->Enqueue((uint64_t)action)) {
                buckets_out[j] += 1;
            }
            else {
                for (int i = 0; i < 110; ++i) {
                    single_work();
                }
            }
            phase_time = rdtsc();
            if (phase_time - start_time >= (FREQUENCY/10)) {
                start_time = phase_time;
                volatile int end_count = NumWorkerDone();
                buckets_done[j] = end_count - start_count;
                start_count = end_count;
                break;
            }            
            for (int i = 0; i < 10000; ++i) {
                single_work();
            }
        }
    }
    
    // Run spike for the next third
    for (; j < 2*duration/3; ++j) {
        while (true) {
            buckets_in[j] += 1;
            EagerAction *action = input_actions[input_index++];
            if (input_queue->Enqueue((uint64_t)action)) {
                buckets_out[j] += 1;
            }                
            else {
                for (int i = 0; i < 500; ++i) {
                    single_work();
                }
            }
                

            phase_time = rdtsc();

            // Check whether or not 1 second has elapsed. 
            // XXX: The constant here is specific to smorz!!!
            if (phase_time - start_time >= (FREQUENCY/10)) {
                start_time = phase_time;
                volatile int end_count = NumWorkerDone();
                buckets_done[j] = end_count - start_count;
                start_count = end_count;

                break;
            }
            for (int i = 0; i < 1300; ++i) {
                single_work();
            }
        }
    }
    
    // Run easy input for the final third
    for (; j < (duration+2*duration/3); ++j) {
        while (true) {
            buckets_in[j] += 1;
            EagerAction *action = input_actions[input_index++];
            if (input_queue->Enqueue((uint64_t)action)) {
                buckets_out[j] += 1;
            }
            else {
                for (int i = 0; i < 110; ++i) {
                    single_work();
                }
            }
            phase_time = rdtsc();
            if (phase_time - start_time >= (uint64_t)(FREQUENCY/10)) {
                start_time = phase_time;
                volatile int end_count = NumWorkerDone();
                buckets_done[j] = end_count - start_count;
                start_count = end_count;
                break;
            }            
            for (int i = 0; i < 10000; ++i) {
                single_work();
            }
        }
    }

    ofstream client_try;
    client_try.open("client_try.txt");
    double cur = 0.0;
    double diff = 1.0 / 10.0;
    for (uint32_t i = 0; i < (duration+2*duration/3); ++i) {
        uint64_t throughput = buckets_in[i]*10;
        client_try << cur << " " << throughput << "\n";
        cur += diff;
    }

    client_try.close();

    ofstream client_success;
    client_success.open("client_success.txt");
    cur = 0.0;
    for (uint32_t i = 0; i < (duration+2*duration/3); ++i) {
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
    for (uint32_t i = 0; i < (duration+2*duration/3); ++i) {
        uint64_t throughput = buckets_done[i]*10;
        throughput_file << cur << " " << throughput << "\n";
        cur += diff;
    }
    throughput_file.close();
}

void
EagerExperiment::RunThroughput() {
    TableInit table_init_params[1];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = m_info->num_records;

    m_lock_mgr = new LockManager(table_init_params, 1);
    
    SimpleQueue **input_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **output_queues = InitQueues(m_info->num_workers, LARGE_QUEUE);
    
    // Initialize the workload generator
    EagerGenerator *gen = NULL;
    if (m_info->is_normal) {
        gen = new EagerNormalGenerator(m_info->read_set_size, 
                                       m_info->write_set_size, 
                                       m_info->num_records, 
                                       m_info->substantiate_period, 
                                       m_info->std_dev);
    }
    else {
        gen = new EagerUniformGenerator(m_info->read_set_size, 
                                        m_info->write_set_size, m_info->num_records, 
                                        m_info->substantiate_period);
    }
    EagerWorker **workers = InitWorkers(m_info->num_workers, input_queues, 
                                        output_queues, 0);
    /*
    SimpleQueue **sched_input = InitQueues(1, LARGE_QUEUE);

    EagerScheduler *sched = new EagerScheduler(m_lock_mgr, sched_input[0], 
                                               input_queues, (uint32_t)m_info->num_workers,
                                               0);
                                               sched->Run();
    */

    InitInputs(input_queues, m_info->num_txns, m_info->num_workers, gen);
    DoThroughputExperiment(workers, output_queues, m_info->num_workers, 
                           (uint32_t)m_info->num_txns);    
    WriteLatencies();        
}

void
EagerExperiment::WriteLatencies() {

    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
            timespec diff = diff_time(m_actions[i]->end_time, m_actions[i]->start_time);            
            times[count++] = (1000000.0*diff.tv_sec) + (diff.tv_nsec/1000.0);
    }
    WriteCDF(times, count);
}


void
EagerExperiment::WriteBlindLatencies() {
    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
        if (dynamic_cast<shopping::EagerCheckout*>(m_actions[i]) != NULL || 
            dynamic_cast<shopping::EagerClearCart*>(m_actions[i]) != NULL) {
            timespec diff = diff_time(m_actions[i]->end_time, m_actions[i]->start_time);            
            times[count++] = (1000000.0*diff.tv_sec) + (diff.tv_nsec/1000.0);
        }
    }
    WriteCDF(times, count);
}
