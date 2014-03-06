#include <lazy_experiment.hh>

LazyExperiment::LazyExperiment(ExperimentInfo *info)
    : Experiment(info) { 
}

uint32_t
LazyExperiment::InitInputs(SimpleQueue *input_queue, int num_inputs, 
                           WorkloadGenerator *gen) {
    uint32_t num_waits = 0;
    for (int i = 0; i < num_inputs; ++i) {
        Action *txn = gen->genNext();
        if (txn->materialize) {
            num_waits += 1;
        }
        input_queue->EnqueueBlocking((uint64_t)txn);
    }
    return num_waits;
}

void
LazyExperiment::RunThroughput() {
    TableInit table_init_params[1];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = m_info->num_records;

    SimpleQueue **input_queue = InitQueues(1, LARGE_QUEUE);
    m_input_queue = input_queue[0];
    
    SimpleQueue **worker_inputs = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **worker_outputs = InitQueues(m_info->num_workers, LARGE_QUEUE);
    m_output_queues = worker_outputs;
    m_workers = (LazyWorker**)malloc(sizeof(LazyWorker*)*m_info->num_workers);

    for (int i = 0; i < m_info->num_workers; ++i) {
        m_workers[i] = new LazyWorker(worker_inputs[i], NULL, worker_outputs[i], 
                                      i+1);
    }
    m_scheduler =  new LazyScheduler(m_input_queue, NULL, worker_inputs, 
                                     (uint32_t)m_info->num_workers, 0, 
                                     table_init_params, 1,
                                     (uint32_t)m_info->substantiate_threshold);
    
    WorkloadGenerator *gen = NULL;
    if (m_info->is_normal) {
        gen = new NormalGenerator(m_info->read_set_size, 
                                  m_info->write_set_size, m_info->num_records, 
                                  m_info->substantiate_period, m_info->std_dev);
    }
    else {
        gen = new UniformGenerator(m_info->read_set_size, 
                                   m_info->write_set_size, m_info->num_records, 
                                   m_info->substantiate_period);
    }
    
    uint32_t num_waits = InitInputs(m_input_queue, m_info->num_txns, gen);
    DoThroughputExperiment(m_info->num_workers, num_waits);
}

/*
void
LazyExperiment::RunYCSB() {
    TableInit table_init_params[1];
    TableInit temp;
    temp.m_table_type = ONE_DIM_TABLE;
    temp.m_params.m_one_params.m_dim1 = m_info->num_records;
    
    SimpleQueue **input_queue = InitQueues(1, LARGE_QUEUE);    
    m_input_queue = input_queue[0];
    
    // Initialize the worker input and output queues, we don't need to include 
    // feedback queues because there are no split transactions for YCSB.
    SimpleQueue **worker_inputs = InitQueues(m_info->num_workers, SMALL_QUEUE);
    SimpleQueue **worker_outputs = InitQueues(m_info->num_workers, SMALL_QUEUE);    
    m_output_queues = worker_outputs;
    m_workers = (LazyWorker**)malloc(sizeof(LazyWorker*)*info->num_workers);

    for (uint32_t i = 0; i < m_info->num_workers; ++i) {
        m_workers[i] = new LazyWorker(worker_inputs[i], NULL, worker_outputs[i], 
                                      i+1);
    }


    m_scheduler =  new LazyScheduler(m_input_queue, feedback, worker_inputs, 
                                     (uint32_t)m_info->num_workers, 0, 
                                     table_init_params, 1,
                                     (uint32_t)m_info->substantiate_threshold);

}
*/

void
LazyExperiment::RunTPCC() {
    InitializeTPCC();
    assert(m_workers != NULL);
    assert(m_scheduler != NULL);
    assert(m_input_queue != NULL);
    assert(m_output_queues != NULL);
    
    // Put txns in the input queue
    TPCCGenerator *txn_generator;
    if (m_info->given_split) {
        assert(false);		// XXX: REMOVE ME
        txn_generator = new TPCCGenerator(m_info->new_order, m_info->district, 
                                          m_info->stock_level, m_info->delivery, 
                                          m_info->order_status);
    }
    else {
        txn_generator = new TPCCGenerator(45, 43, 5, 5, 5);
    }
    uint32_t num_waits = InitInputs(m_input_queue, m_info->num_txns, txn_generator);
    DoThroughputExperiment(m_info->num_workers, num_waits);    
}

void
LazyExperiment::DoThroughputExperiment(int num_workers, uint32_t num_waits) {
    // Start the workers and the scheduler
    for (int i = 0; i < m_info->num_workers; ++i) {
        m_workers[i]->Run();
    }
    
    m_scheduler->Run();

    timespec start_time, end_time;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);

    // Wait for the exeriment to complete
    uint32_t num_done = 0;
    uint32_t num_waits_done = 0;
    std::cout << "To wait: " << num_waits << "\n";
    /*
    while (!m_input_queue->isEmpty())
        ;
    */

    while (num_waits_done < num_waits) {
        for (int i = 0; i < num_workers; ++i) {
            Action *dummy;
            while (m_output_queues[i]->Dequeue((uint64_t*)&dummy)) {
                num_done += 1;
                if (dummy->materialize) {
                    num_waits_done += 1;
                }
            }
        }
    }

    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
    if (num_done < num_waits_done) {
        std::cout << num_done << " " << num_waits_done << "\n";
    }
    assert(num_done >= num_waits_done);
    timespec diff = diff_time(end_time, start_time);
    std::cout << num_done << " " << diff.tv_sec << "." << diff.tv_nsec << "\n";
}

void
LazyExperiment::InitializeTPCCWorkers(uint32_t num_workers, 
                                      SimpleQueue ***inputs, 
                                      SimpleQueue ***feedback, 
                                      SimpleQueue ***outputs) {    
    m_workers = (LazyWorker**)malloc(sizeof(LazyWorker*)*num_workers);

    // Initialize the queues
    *inputs = InitQueues(num_workers, LARGE_QUEUE);
    *feedback = InitQueues(num_workers, LARGE_QUEUE);
    *outputs = InitQueues(num_workers, LARGE_QUEUE);

    // Initialize the workers
    for (uint32_t i = 0; i < num_workers; ++i) {
        m_workers[i] = new LazyWorker((*inputs)[i], (*feedback)[i], (*outputs)[i], 
                                   (int)i+1);
    }
}

// Initializes m_workers and m_scheduler
void
LazyExperiment::InitializeTPCC() {
    using namespace tpcc;

    // Initialize the workers
    SimpleQueue **feedback, **worker_inputs, **worker_outputs;
    InitializeTPCCWorkers((uint32_t)m_info->num_workers, &worker_inputs, 
                          &feedback, &worker_outputs);
    m_output_queues = worker_outputs;
    
    // Initialize the scheduler tables
    TableInit *table_init_params = InitializeTPCCParams();    
    
    // Initialize the scheduler input queue
    SimpleQueue **input_queue = InitQueues(1, LARGE_QUEUE);
    m_input_queue = input_queue[0];
    assert(m_input_queue != NULL);
    
    // Initialize the scheduler
    m_scheduler =  new LazyScheduler(m_input_queue, feedback, worker_inputs, 
                                     (uint32_t)m_info->num_workers, 0, 
                                     table_init_params, s_num_tables, 
                                     (uint32_t)m_info->substantiate_threshold);
}

TableInit*
LazyExperiment::InitializeTPCCParams() {
    using namespace tpcc;
    using namespace cc_params;
    
    TableInit *scheduler_params = 
        (TableInit*)malloc(sizeof(TableInit)*s_num_tables);

    for (int i = 0; i < (int)s_num_tables; ++i) {
        switch (i) {            

        case WAREHOUSE:
            GetWarehouseTableInit(&scheduler_params[i]);
            break;
        case DISTRICT:
            GetDistrictTableInit(&scheduler_params[i]);
            break;
        case CUSTOMER:
            GetCustomerTableInit(&scheduler_params[i]);
            break;
        case HISTORY:
            GetHistoryTableInit(&scheduler_params[i]);
            break;
        case NEW_ORDER:
            GetNewOrderTableInit(1<<24, &scheduler_params[i], false);
            break;
        case OPEN_ORDER:
            GetOpenOrderTableInit(1<<24, &scheduler_params[i], false);
            break;            
        case ORDER_LINE:	// All order lines implicitly sync on open order
            GetEmptyTableInit(&scheduler_params[i]);
            break;
        case ITEM:			// The items table is static, avoid locking it
            GetEmptyTableInit(&scheduler_params[i]);
            break;
        case STOCK:
            GetStockTableInit(&scheduler_params[i]);
            break;
        case OPEN_ORDER_INDEX:
            GetOpenOrderIndexTableInit(&scheduler_params[i]);
            break;
        case NEXT_DELIVERY:
            GetNextDeliveryTableInit(&scheduler_params[i]);
            break;
        default:
            std::cout << "Got an unexpected tpcc table type!\n";
            exit(-1);
        }
    }
    return scheduler_params;
}
