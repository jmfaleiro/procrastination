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


class EagerExperiment {
private:
    ExperimentInfo 		*m_info;
    LockManager			*m_lock_mgr;

    void
    InitializeTPCCLockManager();
    
    SimpleQueue**
    InitQueues(int num_queues, uint32_t size);

    void
    InitInputs(SimpleQueue **input_queues, int num_inputs, EagerGenerator *gen);

    EagerWorker**
    InitWorkers(int num_workers, SimpleQueue **input_queues, 
                SimpleQueue **output_queues);

    void
    DoThroughputExperiment(EagerWorker **workers, SimpleQueue **output_queues, 
                           int num_workers, uint32_t num_waits);

    void
    RunTPCC();


public:
    EagerExperiment(ExperimentInfo *info);
    
    void
    Run();
};

#endif 		// EAGER_EXPERIMENT_HH_
