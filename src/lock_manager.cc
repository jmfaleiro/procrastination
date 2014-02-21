// Author: Kun Ren (kun@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.
//
// Adapted by Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#include <lock_manager.hh>

LockManager::LockManager(uint64_t table_size) {
    assert(!(m_size & (m_size-1)));
    m_lock_table = (deque<KeysList>**)malloc(table_size*CACHE_LINE);
    m_size = table_size;
}

void
LockManager::Acquire(uint64_t index) {
    assert(index < m_size);
    uint64_t *lock_addr = 
        (uint64_t*)(((char*)m_lock_table)+index*CACHE_LINE+
                    sizeof(deque<KeysList>*));
    lock(lock_addr);
}                    

void
LockManager::Release(uint64_t index) {
    assert(index < m_size);
    uint64_t *lock_addr = 
        (uint64_t*)(((char*)m_lock_table)+index*CACHE_LINE+
                    sizeof(deque<KeysList>*));
    unlock(lock_addr);
}

void
LockManager::LockWrite(CompositeKey key, EagerAction *txn) {
    uint64_t index = hash(key) & (m_size-1);
    Acquire(index);
    
    deque<KeysList> *key_requests = 
        *(deque<KeysList>**)(((char*)m_lock_table) + index*CACHE_LINE);
    deque<KeysList>::iterator it;
    for (it = key_requests->begin(); 
         it != key_requests->end() && it->key != key; ++it)
        ;
    deque<LockRequest> *requests = NULL;
    if (it == key_requests->end()) {
        requests = new deque<LockRequest>();
        key_requests->push_back(KeysList(key, requests));
    }
    else {
        requests = it->locksrequest;
    }

    requests->push_back(LockRequest(true, txn));
    
    // Spin waiting for the lock holder to wake us up
    if (requests->size() > 1) {
        Release(index);
        txn->Spin(NULL);
    }
}

void
LockManager::LockRead(CompositeKey key, EagerAction *txn) {
    uint64_t index = hash(key) & (m_size-1);
    Acquire(index);
    
    deque<KeysList> *key_requests = 
        *(deque<KeysList>**)(((char*)m_lock_table) + index*CACHE_LINE);
    deque<KeysList>::iterator it;
    for (it = key_requests->begin(); 
         it != key_requests->end() && it->key != key; ++it)
        ;
    deque<LockRequest> *requests = NULL;
    if (it == key_requests->end()) {
        requests = new deque<LockRequest>();
        key_requests->push_back(KeysList(key, requests));
    }
    else {
        requests = it->locksrequest;
    }

    requests->push_back(LockRequest(false, txn));
    for (deque<LockRequest>::iterator it = requests->begin();
         it != requests->end(); ++it) {
        if (it->is_write == true) {
            Release(index);
            txn->Spin(NULL);
        }
    }
}

void
LockManager::Unlock(CompositeKey key, EagerAction *txn) {
    uint64_t index = hash(key) & (m_size-1);
    Acquire(index);
    
    deque<KeysList> *key_requests = 
        *(deque<KeysList>**)(((char*)m_lock_table) + index*CACHE_LINE);
    deque<KeysList>::iterator it1;
    for (it1 = key_requests->begin(); 
         it1 != key_requests->end() && it1->key != key; ++it1)
        ;
    
    // Better be the case that we found something...
    assert(it1->key == key);
    deque<LockRequest>* requests = it1->locksrequest;
    deque<LockRequest>::iterator it;
    for (it = requests->begin(); it != requests->end() && it->txn != txn; 
         ++it) {
        
        // Make sure no writes precede this txn
        assert(!it->is_write);	
    }
    
    // Better have found ourselves in the chain...
    assert(it != requests->end());
    
    // Remember an iterator to delete the element from the chain
    deque<LockRequest>::iterator target = it;
    ++it;
    if (it != requests->end()) {
        if (target == requests->begin() &&
            (target->is_write ||
             (!target->is_write && it->is_write))) {
            if (it->is_write) {
                it->txn->Wakeup(NULL);
            }
            
            for (; it != requests->end() && !it->is_write; ++it) {
                it->txn->Wakeup(NULL);
            }
        }        
    }
    
    requests->erase(target);
    Release(index);
}
