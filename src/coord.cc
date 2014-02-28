#include "worker.h"
#include "lazy_scheduler.hh"
#include "cpuinfo.h"
#include "concurrent_queue.h"
#include "action.h"
#include "normal_generator.h"
#include "uniform_generator.h"
#include "experiment_info.h"
#include "machine.h"
#include "client.h"
#include "shopping_cart.h"
#include <tpcc_generator.hh>

#include <string_table.hh>

#include <tpcc.hh>


#include <numa.h>
#include <pthread.h>
#include <sched.h>

#include <eager_tpcc_generator.hh>
#include <lock_manager_test.hh>

#include <eager_experiment.hh>

#include <iostream> 
#include <fstream>
#include <time.h>
#include <algorithm>
#include <sstream>
#include <string>
#include <iomanip>

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    if (info->serial) {
        EagerExperiment *expt = new EagerExperiment(info);
        expt->Run();
    }

    //    run_experiment(info);
    return 0;
}
