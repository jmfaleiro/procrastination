#include <experiment_info.h>
#include <experiment.hh>
#include <eager_experiment.hh>
#include <lazy_experiment.hh>

int
main(int argc, char** argv) {
    ExperimentInfo* info = new ExperimentInfo(argc, argv);
    Experiment *expt;
    if (info->serial) {
        expt = new EagerExperiment(info);        
    }
    else {
        expt = new LazyExperiment(info);
    }    
    expt->Run();
    return 0;
}
