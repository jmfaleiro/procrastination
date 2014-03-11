#include <lazy_experiment.hh>

LazyExperiment::LazyExperiment(ExperimentInfo *info)
    : Experiment(info) { 
}

uint32_t
LazyExperiment::InitInputs(SimpleQueue *input_queue, int num_inputs, 
                           WorkloadGenerator *gen) {
    uint32_t num_waits = 0;
    m_actions = (Action**)malloc(sizeof(Action*)*num_inputs);
    timespec zero_time;
    zero_time.tv_sec = 0;
    zero_time.tv_nsec = 0;
    for (int i = 0; i < num_inputs; ++i) {
        //        std::cout << i << "\n";
        Action *txn = gen->genNext();
        if (m_info->blind_write_frequency == -1) {
            if (i >= num_inputs - m_info->num_workers -1) {
                txn->materialize = true;
            }
        }
        if (txn->materialize) {
            num_waits += 1;
        }
        m_actions[i] = txn;
        m_actions[i]->start_time = zero_time;
        m_actions[i]->end_time = zero_time;
        m_actions[i]->start_rdtsc_time = 0;
        m_actions[i]->end_rdtsc_time = 0;
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
    SimpleQueue **feedbacks = InitQueues(m_info->num_workers, LARGE_QUEUE);
    m_output_queues = worker_outputs;
    m_workers = (LazyWorker**)malloc(sizeof(LazyWorker*)*m_info->num_workers);

    for (int i = 0; i < m_info->num_workers; ++i) {
        m_workers[i] = new LazyWorker(worker_inputs[i], feedbacks[i], worker_outputs[i], 
                                      i+1);
    }
    m_scheduler =  new LazyScheduler(m_input_queue, feedbacks, worker_inputs, 
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
    WriteLatencies();
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
LazyExperiment::RunBlind() {
    TableInit table_init_params[2];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = 2000;

    table_init_params[1].m_table_type = ONE_DIM_TABLE;
    table_init_params[1].m_params.m_one_params.m_dim1 = 1000000;

    SimpleQueue **input_queue = InitQueues(1, LARGE_QUEUE);
    m_input_queue = input_queue[0];
    
    SimpleQueue **worker_inputs = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **worker_outputs = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **feedbacks = InitQueues(m_info->num_workers, LARGE_QUEUE);
    m_output_queues = worker_outputs;
    m_workers = (LazyWorker**)malloc(sizeof(LazyWorker*)*m_info->num_workers);

    for (int i = 0; i < m_info->num_workers; ++i) {
        m_workers[i] = new LazyWorker(worker_inputs[i], feedbacks[i], worker_outputs[i], 
                                      i+1);
    }
    m_scheduler =  new LazyScheduler(m_input_queue, feedbacks, worker_inputs, 
                                     (uint32_t)m_info->num_workers, 0, 
                                     table_init_params, 2,
                                     (uint32_t)m_info->substantiate_threshold);

    WorkloadGenerator *gen = new ShoppingCart(2000, 1000000, 20, m_info->blind_write_frequency, 0);
    uint32_t num_waits = InitInputs(m_input_queue, m_info->num_txns, gen);
    DoThroughputExperiment(m_info->num_workers, num_waits);
    WriteLatencies();
}

void
LazyExperiment::WriteStockCDF() {
    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
        if (dynamic_cast<StockLevelTxn0*>(m_actions[i]) != NULL) {
            StockLevelTxn0 *level0 = (StockLevelTxn0*)m_actions[i];
            Action *level1, *level2;
            level0->IsLinked(&level1);
            level1->IsLinked(&level2);
            assert((level0->start_time.tv_sec != 0 || level0->start_time.tv_nsec != 0) &&
                   (level2->start_time.tv_sec != 0 || level2->start_time.tv_nsec != 0));                   
            timespec diff = diff_time(level2->end_time, level0->start_time);
            times[count++] = (1000000.0*diff.tv_sec) + (diff.tv_nsec/1000.0);
        }
    }
    WriteCDF(times, count);
}

void
LazyExperiment::WriteNewOrderCDF() {
    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
        if (dynamic_cast<NewOrderTxn*>(m_actions[i]) != NULL) {
            assert(m_actions[i]->end_rdtsc_time != 0 && m_actions[i]->start_rdtsc_time != 0);
            times[count++] = (1.0*(m_actions[i]->end_rdtsc_time - m_actions[i]->start_rdtsc_time)) / 1996.0;
        }
    }
    WriteCDF(times, count);
}

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
    //    WriteStockCDF();
    //    WriteNewOrderCDF();
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
    WriteThroughput(diff, num_done);
    std::cout << num_done << " " << diff.tv_sec << "." << diff.tv_nsec << "\n";
}

void
LazyExperiment::WriteLatencies() {
    double *times = (double*)malloc(sizeof(double)*m_info->num_txns);
    int count = 0;
    for (int i = 0; i < m_info->num_txns; ++i) {
        if (m_actions[i]->materialize && 
            (m_actions[i]->start_time.tv_sec != 0 || m_actions[i]->start_time.tv_nsec != 0) && 
            (m_actions[i]->end_time.tv_sec != 0 || m_actions[i]->end_time.tv_nsec != 0)) {
         
            timespec diff = diff_time(m_actions[i]->end_time, m_actions[i]->start_time);            
            if (diff.tv_sec >= 0 && diff.tv_nsec >= 0) {
                times[count++] = (1000000.0*diff.tv_sec) + (diff.tv_nsec/1000.0);
            }
        }
    }
    WriteCDF(times, count);    
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

uint32_t 
LazyExperiment::NumWorkerDone() {
    uint64_t ret = 0;
    for (int i = 0; i < m_info->num_workers; ++i) {
        ret += m_workers[i]->NumDone();
    }
    return ret;
}


void
LazyExperiment::RunPeak() {
    TableInit table_init_params[1];
    table_init_params[0].m_table_type = ONE_DIM_TABLE;
    table_init_params[0].m_params.m_one_params.m_dim1 = m_info->num_records;
    
    // Use a uniform distribution on keys
    WorkloadGenerator *gen = new UniformGenerator(m_info->read_set_size, 
                                                  m_info->write_set_size, 
                                                  m_info->num_records, 
                                                  m_info->substantiate_period);

    // Generate the input actions
    Action **input_actions = 
        (Action**)malloc(sizeof(Action*)*m_info->num_txns);
    for (int i = 0; i < m_info->num_txns; ++i) {
        input_actions[i] = gen->genNext();
    }
    
    // Create the workers
    SimpleQueue **worker_inputs = InitQueues(m_info->num_workers, SMALL_QUEUE);
    SimpleQueue **worker_outputs = InitQueues(m_info->num_workers, LARGE_QUEUE);
    SimpleQueue **feedbacks = InitQueues(m_info->num_workers, SMALL_QUEUE);
    m_output_queues = worker_outputs;
    m_workers = (LazyWorker**)malloc(sizeof(LazyWorker*)*m_info->num_workers);
    

    for (int i = 0; i < m_info->num_workers; ++i) {
        m_workers[i] = new LazyWorker(worker_inputs[i], feedbacks[i], worker_outputs[i], 
                                      i+1);
    }

    SimpleQueue **sched_input = InitQueues(1, SMALL_QUEUE);
    m_scheduler =  new LazyScheduler(sched_input[0], feedbacks, worker_inputs, 
                                     (uint32_t)m_info->num_workers, 0, 
                                     table_init_params, 1,
                                     (uint32_t)m_info->substantiate_threshold);

    // Do the experiment
    WaitPeak(180, input_actions, sched_input[0]);
}

void
LazyExperiment::WaitPeak(uint32_t duration,
                         Action **input_actions, 
                         SimpleQueue *input_queue) {

    if (pin_thread(m_info->num_workers+1) == -1) {
        std::cout << "Lazy experiment: Client thread couldn't bind to cpu!!!";
        std::cout << "\n";
        exit(-1);
    }

    m_scheduler->Run();
    for (int i = 0; i < m_info->num_workers; ++i) {
        m_workers[i]->Run();
    }

    volatile uint64_t start_time = rdtsc();
    volatile uint64_t phase_time; 
    duration *= 10;

    uint64_t* buckets_in = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_in, 0, sizeof(uint64_t)*(duration+2*duration/3));

    uint64_t* buckets_stick = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_stick, 0, sizeof(uint64_t)*(duration+2*duration/3));
    

    uint64_t* buckets_out = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_out, 0, sizeof(uint64_t)*(duration+2*duration/3));
        
    uint64_t* buckets_done = 
        (uint64_t*)numa_alloc_local(sizeof(uint64_t)*(duration+2*duration/3));
    memset(buckets_done, 0, sizeof(uint64_t)*(duration+2*duration/3));
        
    volatile uint64_t start_count = NumWorkerDone();
    volatile uint64_t start_stick = m_scheduler->NumStickified();

    // Run easy input for the first third of the experiment
    uint64_t input_index = 0;
    uint32_t j = 0;
    for (; j < duration/3; ++j) {
        while (true) {
            buckets_in[j] += 1;
            Action *action = input_actions[input_index++];
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

                volatile uint64_t end_stick = m_scheduler->NumStickified();
                buckets_stick[j] = end_stick - start_stick;
                start_stick = end_stick;
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
            Action *action = input_actions[input_index++];
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

                volatile uint64_t end_stick = m_scheduler->NumStickified();
                buckets_stick[j] = end_stick - start_stick;
                start_stick = end_stick;

                break;
            }
            for (int i = 0; i < 1500; ++i) {
                single_work();
            }
        }
    }
    
    // Run easy input for the final third
    for (; j < duration+2*duration/3; ++j) {
        while (true) {
            buckets_in[j] += 1;
            Action *action = input_actions[input_index++];
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

                volatile uint64_t end_stick = m_scheduler->NumStickified();
                buckets_stick[j] = end_stick - start_stick;
                start_stick = end_stick;
                break;
            }            
            for (int i = 0; i < 10000; ++i) {
                single_work();
            }
        }
    }

    ofstream client_try;
    client_try.open("lazy_client_try.txt");
    double cur = 0.0;
    double diff = 1.0 / 10.0;
    for (uint32_t i = 0; i < (duration+2*duration/3); ++i) {
        uint64_t throughput = buckets_in[i]*10;
        client_try << cur << " " << throughput << "\n";
        cur += diff;
    }

    client_try.close();

    ofstream stick_file;
    stick_file.open("lazy_client_stickification.txt");
    cur = 0.0;
    for (uint32_t i = 0; i < (duration+2*duration/3); ++i) {
        uint64_t stick = buckets_stick[i]*10;
        stick_file << cur << " " << stick << "\n";
        cur += diff;
    }
    stick_file.close();

    ofstream client_success;
    client_success.open("lazy_client_success.txt");
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
    throughput_file.open("lazy_client_throughput.txt");
    cur = 0.0;
    for (uint32_t i = 0; i < (duration+2*duration/3); ++i) {
        uint64_t throughput = buckets_done[i]*10;
        throughput_file << cur << " " << throughput << "\n";
        cur += diff;
    }
    throughput_file.close();
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
