#ifndef 		LOCK_MANAGER_TEST_HH_
#define 		LOCK_MANAGER_TEST_HH_

#include <cassert>
#include <lock_manager.hh>
#include <concurrency_control_params.hh>

class LockManagerTest {

private:
    LockManager *mgr;

public:
    LockManagerTest();
        
    void
    TestConflictSerial();
    
    void
    TestReleaseSerial();

    void
    TestConflictParallel();
    
    void
    TestReleaseParallel();
};

#endif 		 // LOCK_MANAGER_TEST_HH_
