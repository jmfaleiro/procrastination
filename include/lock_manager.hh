#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <action.h>
#include <tpcc.hh>
#include <action.h>
#include <deque>
#include <concurrency_control_params.hh>

using namespace std;

struct TxnQueue {
    struct DependencyInfo												*head;
    struct DependencyInfo												**tail;
    volatile uint64_t __attribute((aligned(CACHE_LINE))) 	lock_word;
    
    TxnQueue() {
        head = NULL;
        tail = NULL;
        lock_word = 0;
    }
};

class LockManager {    
private:
    Table<uint64_t, TxnQueue>		**m_tables;    

    bool
    CheckWrite(struct TxnQueue *queue, struct DependencyInfo *dep);

    bool
    CheckRead(struct TxnQueue *queue, struct DependencyInfo *dep);

    void
    AddTxn(struct TxnQueue *queue, struct DependencyInfo *dep);

    void
    RemoveWrite(struct TxnQueue *queue, struct DependencyInfo *dep);

    void
    RemoveRead(struct TxnQueue *queue, struct DependencyInfo *dep);

public:
    LockManager(cc_params::TableInit *params, int num_params);
    
    // Acquire and release the mutex protecting a particular hash chain
    virtual void
    Unlock(Action *txn);

    virtual bool
    Lock(Action *txn);
};

#endif // LOCK_MANAGER_HH_
