#ifndef 		LOCK_MANAGER_TEST_HH_
#define 		LOCK_MANAGER_TEST_HH_

#include <cassert>
#include <lock_manager.hh>
#include <concurrency_control_params.hh>

class LockManagerTest {

private:
    LockManager *mgr;

    void
    TestConflictSerial();

public:
    LockManagerTest();
    
    void
    DoTest();
};

#endif 		 // LOCK_MANAGER_TEST_HH_
