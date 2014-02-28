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

class EagerExperiment : public Experiment {
private:
    LockManager			*m_lock_mgr;

    void
    InitializeTPCCLockManager();

    void
    InitInputs(SimpleQueue **input_queues, int num_inputs, int num_workers, 
               EagerGenerator *gen);

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
};

#endif 		// EAGER_EXPERIMENT_HH_
