#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <action.h>
#include <tpcc.hh>
#include <action.h>
#include <deque>

using namespace std;

struct LockRequest {
    LockRequest(bool w, EagerAction* t) : txn(t), is_write(w) {}
    EagerAction* txn;  // Pointer to txn requesting the lock.
    bool is_write;  // Specifies whether this is a read or write lock request.
};

struct KeysList {
    KeysList(CompositeKey m, deque<LockRequest>* t) : key(m), locksrequest(t) {}
    CompositeKey key;
    deque<LockRequest>* locksrequest;
};

class LockManager {
    
private:
    static uint64_t 
    hash(CompositeKey key) {
        char *start = (char*)&key;
        return CityHash64(start, sizeof(CompositeKey));
    }

    uint64_t 				m_size;
    deque<KeysList> 		**m_lock_table;

public:
    LockManager(uint64_t table_size);
    
    // Acquire and release the mutex protecting a particular hash chain
    virtual void Acquire(uint64_t index);
    virtual void Release(uint64_t index);

    virtual void LockWrite(CompositeKey key, EagerAction* txn);
    virtual void LockRead(CompositeKey key, EagerAction *txn);
    virtual void Unlock(CompositeKey key, EagerAction *txn);
};

#endif // LOCK_MANAGER_HH_
