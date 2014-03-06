#include <eager_experiment.hh>

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
    for (int i = 0; i < num_inputs; ++i) {
        input_queues[i%num_workers]->EnqueueBlocking((uint64_t)gen->genNext());
    }
}

EagerWorker**
EagerExperiment::InitWorkers(int num_workers, SimpleQueue **input_queues, 
                             SimpleQueue **output_queues) {
    EagerWorker **ret = new EagerWorker*[num_workers];
    for (int i = 0; i < num_workers; ++i) {
        ret[i] = new EagerWorker(m_lock_mgr, input_queues[i], output_queues[i], 
                                 i);
    }
    return ret;
}

void
EagerExperiment::DoThroughputExperiment(EagerWorker **workers, 
                                        SimpleQueue **output_queues, 
                                        int num_workers, uint32_t num_waits) {
    
    // Bind this thread to a particular cpu to avoid interfering with workers
    if (pin_thread(num_workers) == -1) {
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
    std::cout << diff.tv_sec << "." << diff.tv_nsec << "\n";
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
        // txn_generator = EagerTPCCGenerator(45, 43, 5, 5, 5);
        txn_generator = EagerTPCCGenerator(45, 43, 5, 5, 5);
    }
    InitInputs(input_queues, m_info->num_txns, m_info->num_workers, &txn_generator);    
    EagerWorker **workers = InitWorkers(m_info->num_workers, input_queues, 
                                        output_queues);

    
    DoThroughputExperiment(workers, output_queues, m_info->num_workers, 
                           (uint32_t)m_info->num_txns);    
}

void
EagerExperiment::RunThroughput() {
    TableInit table_init_params[1];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = m_info->num_records;
    
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
    InitInputs(input_queues, m_info->num_txns, m_info->num_workers, gen);    
    EagerWorker **workers = InitWorkers(m_info->num_workers, input_queues, 
                                        output_queues);

    
    DoThroughputExperiment(workers, output_queues, m_info->num_workers, 
                           (uint32_t)m_info->num_txns);    
}
