#ifndef 		EXPERIMENT_HH_
#define			EXPERIMENT_HH_

#include <experiment_info.h>
#include <concurrent_queue.h>
#include <machine.h>
#include <time.h>
#include <tpcc.hh>

class Experiment {
protected:
    ExperimentInfo 			*m_info;    
    
    static timespec
    diff_time(timespec end, timespec start);

    SimpleQueue**
    InitQueues(int num_queues, uint32_t size);
    
    virtual void RunTPCC() = 0;

public:
    Experiment(ExperimentInfo *info);

    virtual void Run();
};

#endif 		//  EXPERIMENT_HH_
