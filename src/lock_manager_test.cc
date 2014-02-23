#include <lock_manager_test.hh>
#include <tpcc.hh>

using namespace cc_params;

LockManagerTest::LockManagerTest() {
    
    TableInit *init_params = (TableInit*)malloc(sizeof(TableInit)*4);
    memset(init_params, 0, sizeof(TableInit)*4);

    init_params[0].m_table_type = HASH_TABLE;
    init_params[0].m_params.m_hash_params = {
        .m_size = 1<<8, 
        .m_chain_bound = 10,
    };
    
    init_params[1].m_table_type = ONE_DIM_TABLE;
    init_params[1].m_params.m_one_params = {
        .m_dim1 = 10,
    };

    init_params[2].m_table_type = TWO_DIM_TABLE;
    init_params[2].m_params.m_two_params = {
        .m_dim1 = 10, 
        .m_dim2 = 10, 
        .m_access1 = TPCCKeyGen::get_warehouse_key,
        .m_access2 = TPCCKeyGen::get_district_key,
    };

    init_params[3].m_table_type = THREE_DIM_TABLE;
    init_params[3].m_params.m_three_params = {
        .m_dim1 = 10, 
        .m_dim2 = 10, 
        .m_dim3 = 10, 
        .m_access1 = TPCCKeyGen::get_warehouse_key,
        .m_access2 = TPCCKeyGen::get_district_key,
        .m_access3 = TPCCKeyGen::get_customer_key,
    };
    
    mgr = new LockManager(init_params, 4);
}

void
LockManagerTest::DoTest() {
    TestConflictSerial();
}

void
LockManagerTest::TestConflictSerial() {
    struct DependencyInfo info;
    Action *action1 = new Action();

    info.record.m_table = 0;
    info.record.m_key = 0;
    action1->writeset.push_back(info);
    
    info.record.m_table = 1;
    info.record.m_key = 0;
    action1->writeset.push_back(info);
    
    bool decision = mgr->Lock(action1);
    assert(decision == true);
    
    Action *action2 = new Action();
    
    info.record.m_table = 0;
    info.record.m_key = 0;
    action2->writeset.push_back(info);
    
    info.record.m_table = 1;
    info.record.m_key = 0;
    action2->writeset.push_back(info);
    
    decision = mgr->Lock(action2);
    assert(decision == false);
    assert(action2->num_dependencies == 2);
    
    mgr->Unlock(action1);
    assert(action2->num_dependencies == 0);
}
