#ifndef 		LAZY_EXPERIMENT_HH_
#define 		LAZY_EXPERIMENT_HH_

#include <experiment.hh>
#include <experiment_info.h>
#include <concurrency_control_params.hh>
#include <tpcc_table_spec.hh>
#include <lazy_scheduler.hh>
#include <lazy_worker.hh>
#include <machine.h>
#include <concurrent_queue.h>
#include <workload_generator.h>
#include <tpcc_generator.hh>
#include <normal_generator.h>
#include <uniform_generator.h>


class LazyExperiment : public Experiment {
private:
    LazyWorker				**m_workers;
    LazyScheduler 			*m_scheduler;
    SimpleQueue 			*m_input_queue;
    SimpleQueue 			**m_output_queues;

    uint32_t
    InitInputs(SimpleQueue *input_queue, int num_inputs, 
               WorkloadGenerator *gen);

    void
    DoThroughputExperiment(int num_workers, uint32_t num_waits);
    
    void
    InitializeTPCCWorkers(uint32_t num_workers, SimpleQueue ***inputs, 
                          SimpleQueue ***feedback, SimpleQueue ***outputs);
    
    void
    InitializeTPCC();

    TableInit*
    InitializeTPCCParams();

    void
    InitializeTPCCScheduler();
    
protected:

    virtual void
    RunTPCC();

    virtual void
    RunThroughput();
    
public:
    LazyExperiment(ExperimentInfo *info);    
};


#endif 		//  LAZY_EXPERIMENT_HH_
