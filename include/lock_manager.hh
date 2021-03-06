#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <action.h>
#include <tpcc.hh>
#include <deque>
#include <concurrency_control_params.hh>
#include <pthread.h>

using namespace std;

struct TxnQueue {
    struct EagerRecordInfo												*head;
    struct EagerRecordInfo												*tail;
    volatile uint64_t __attribute((aligned(CACHE_LINE))) 				lock_word;
    //    pthread_mutex_t														mutex;

    TxnQueue() {
        head = NULL;
        tail = NULL;
        lock_word = 0;
        //        mutex = PTHREAD_MUTEX_INITIALIZER;
    }
};

class LockManager {    
private:
    Table<uint64_t, TxnQueue>		**m_tables;    

    bool
    CheckWrite(struct TxnQueue *queue, struct EagerRecordInfo *dep);

    bool
    CheckRead(struct TxnQueue *queue, struct EagerRecordInfo *dep);

    void
    AddTxn(struct TxnQueue *queue, struct EagerRecordInfo *dep);

    void
    RemoveTxn(struct TxnQueue *queue, 
              struct EagerRecordInfo *dep, 
              struct EagerRecordInfo **prev,
              struct EagerRecordInfo **next);

    void
    AdjustRead(struct EagerRecordInfo *dep);

    void
    AdjustWrite(struct EagerRecordInfo *dep);

    bool
    QueueContains(TxnQueue *queue, EagerAction *txn);

    void
    FinishAcquisitions(EagerAction *txn);

    void
    LockRecord(EagerAction *txn, struct EagerRecordInfo *dep);

public:
    LockManager(cc_params::TableInit *params, int num_params);
    
    // Acquire and release the mutex protecting a particular hash chain
    virtual void
    Unlock(EagerAction *txn);

    virtual bool
    Lock(EagerAction *txn);
    
    virtual void
    Kill(EagerAction *txn);

    bool
    CheckLocks(EagerAction *txn);
};

#endif // LOCK_MANAGER_HH_
