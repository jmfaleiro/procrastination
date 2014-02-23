#include <lock_manager_test.hh>
#include <tpcc.hh>

using namespace cc_params;

LockManagerTest::LockManagerTest() {
    
    TableInit *init_params = (TableInit*)malloc(sizeof(TableInit)*4);
    memset(init_params, 0, sizeof(TableInit)*4);

    init_params[0].m_table_type = HASH_TABLE;
    init_params[0].m_params.m_hash_params.m_size = 1<<8;
    init_params[0].m_params.m_hash_params.m_chain_bound = 10;

    init_params[1].m_table_type = ONE_DIM_TABLE;
    init_params[1].m_params.m_one_params.m_dim1 = 10;

    init_params[2].m_table_type = TWO_DIM_TABLE;
    init_params[2].m_params.m_two_params.m_dim1 = 10;
    init_params[2].m_params.m_two_params.m_dim2 = 10;
    init_params[2].m_params.m_two_params.m_access1 = TPCCKeyGen::get_warehouse_key;
    init_params[2].m_params.m_two_params.m_access2 = TPCCKeyGen::get_district_key;

    init_params[3].m_table_type = THREE_DIM_TABLE;
    init_params[3].m_params.m_three_params.m_dim1 = 10;
    init_params[3].m_params.m_three_params.m_dim2 = 10;
    init_params[3].m_params.m_three_params.m_dim3 = 10;
    init_params[3].m_params.m_three_params.m_access1 = TPCCKeyGen::get_warehouse_key;
    init_params[3].m_params.m_three_params.m_access2 = TPCCKeyGen::get_warehouse_key;
    init_params[3].m_params.m_three_params.m_access3 = TPCCKeyGen::get_warehouse_key;

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
    
    // Lock Action1
    bool decision = mgr->Lock(action1);
    assert(decision == true);
    
    Action *action2 = new Action();
    
    info.record.m_table = 0;
    info.record.m_key = 0;
    action2->readset.push_back(info);
    
    info.record.m_table = 1;
    info.record.m_key = 0;
    action2->writeset.push_back(info);

    info.record.m_table = 2;
    info.record.m_key = 0;
    action2->writeset.push_back(info);

    info.record.m_table = 3;
    info.record.m_key = 0;
    action2->writeset.push_back(info);
    
    // Locks: Action1 --> Action2
    decision = mgr->Lock(action2);
    assert(decision == false);
    assert(action2->num_dependencies == 2);
    
    Action *action3 = new Action();
    info.record.m_table = 0;
    info.record.m_key = 0;
    action3->readset.push_back(info);
    
    // Locks: Action1 --> Action2 --> Action3
    decision = mgr->Lock(action3);
    assert(decision == false);
    assert(action3->num_dependencies == 1);

    // Locks: Action2 --> Action3
    mgr->Unlock(action1);
    assert(action2->num_dependencies == 0);
    assert(action3->num_dependencies == 0);
    
    // Locks: Action2 --> Action3 --> Action1
    decision = mgr->Lock(action1);
    assert(decision == false);
    assert(action1->num_dependencies == 2);

    // Locks: Action2 --> Action1
    assert(action3->num_dependencies == 0);
    mgr->Unlock(action3);
    assert(action1->num_dependencies == 2);
    
    // Locks: Action1
    mgr->Unlock(action2);
    assert(action1->num_dependencies == 0);
    
    // Locks: 
    mgr->Unlock(action1);    

    info.record.m_table = 2;
    info.record.m_key = 0;
    action3->writeset.push_back(info);

    // Locks: Action1
    decision = mgr->Lock(action1);
    assert(decision);
    assert(action1->num_dependencies == 0);

    // Locks: Action1 --> Action3
    decision = mgr->Lock(action3);
    assert(!decision);
    assert(action3->num_dependencies == 1);

    // Locks: Action1 --> Action3 --> Action2
    decision = mgr->Lock(action2);
    assert(!decision);
    assert(action2->num_dependencies == 3);
    
    // Locks: Action1 --> Action2
    mgr->Kill(action3);
    assert(action2->num_dependencies == 2);
    
    // Locks: Action2
    mgr->Unlock(action1);
    assert(action2->num_dependencies == 0);
    
    mgr->Unlock(action2);
}
