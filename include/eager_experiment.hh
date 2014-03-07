#ifndef 	EAGER_EXPERIMENT_HH_
#define 	EAGER_EXPERIMENT_HH_

#include <experiment_info.h>
#include <eager_tpcc_generator.hh>
#include <eager_worker.hh>
#include <lock_manager.hh>
#include <concurrency_control_params.hh>
#include <cpuinfo.h>
#include <tpcc_table_spec.hh>
#include <concurrent_queue.h>
#include <time.h>
#include <experiment.hh>
#include <normal_generator.h>
#include <uniform_generator.h>
#include <eager_scheduler.hh>
#include <iostream>
#include <fstream>

class EagerExperiment : public Experiment {
private:
    LockManager			*m_lock_mgr;
    EagerWorker			**m_workers;

    void
    InitializeTPCCLockManager();

    void
    InitInputs(SimpleQueue **input_queues, int num_inputs, int num_workers, 
               EagerGenerator *gen);

    EagerWorker**
    InitWorkers(int num_workers, SimpleQueue **input_queues, 
                SimpleQueue **output_queues, int cpu_offset);

    void
    DoThroughputExperiment(EagerWorker **workers, SimpleQueue **output_queues, 
                           int num_workers, uint32_t num_waits);

    void
    RunTPCC();

    void
    RunThroughput();

    void
    RunPeak();
    
    void
    WaitPeak(uint32_t duration,
             EagerAction **input_actions, 
             SimpleQueue *input_queue);

    inline uint32_t
    NumWorkerDone();

public:
    EagerExperiment(ExperimentInfo *info);
};

#endif 		// EAGER_EXPERIMENT_HH_
