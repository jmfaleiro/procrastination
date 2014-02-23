// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#include <lock_manager.hh>

using namespace cc_params;

LockManager::LockManager(TableInit *params, int num_params) {
    m_tables = do_tbl_init<TxnQueue>(params, num_params);
    assert(m_tables != NULL);
}

bool
LockManager::CheckWrite(struct TxnQueue *queue, struct DependencyInfo *dep) {
    bool ret = (queue->head == dep);
    assert(!ret || (dep->prev == NULL));
    return ret;
}

bool
LockManager::CheckRead(struct TxnQueue *queue, struct DependencyInfo *dep) {
    for (struct DependencyInfo *iter = queue->head; 
         iter != dep; iter = iter->next) {
        if (iter->is_write) {
            return false;
        }
    }    
}

void
LockManager::AddTxn(struct TxnQueue *queue, struct DependencyInfo *dep) {
    dep->next = NULL;
    dep->prev = NULL;

    if (queue->tail != NULL) {
        assert(queue->head != NULL);
        dep->prev = queue->tail;
        queue->tail->next = dep;
        queue->tail = dep;
        dep->next = NULL;
    }
    else {
        assert(queue->head == NULL);
        queue->head = dep;
        queue->tail = dep;
        dep->prev = NULL;
        dep->next = NULL;
    }
}

void
LockManager::RemoveTxn(struct TxnQueue *queue, 
                       struct DependencyInfo *dep,
                       struct DependencyInfo **prev_txn,
                       struct DependencyInfo **next_txn) {
    
    struct DependencyInfo *prev = dep->prev;
    struct DependencyInfo *next = dep->next;

    // If we're at either end of the queue, make appropriate adjustments
    if (prev == NULL) {
        assert(queue->head == dep);
        queue->head = next;
    }
    else {
        assert(prev->next == dep);
        prev->next = next;
    }

    if (next == NULL) {
        assert(queue->tail == dep);
        queue->tail = prev;
    }
    else {
        assert(next->prev == dep);
        next->prev = prev;
    }
    *prev_txn = prev;
    *next_txn = next;
}

void
LockManager::AdjustWrite(struct DependencyInfo *dep) {
    assert(dep->is_write);
    struct DependencyInfo *next = dep->next;
    if (next != NULL) {
        if (next->is_write) {
            next->is_held = true;
            fetch_and_decrement(&next->dependency->num_dependencies);
        }
        else {
            for (struct DependencyInfo *iter = next;
                 iter != NULL && !iter->is_write; iter = iter->next) {
                iter->is_held = true;
                fetch_and_decrement(&iter->dependency->num_dependencies);
            }
        }
    }
}

void
LockManager::AdjustRead(struct DependencyInfo *dep) {
    assert(!dep->is_write);
    struct DependencyInfo *next = dep->next;
    if (next != NULL && dep->prev == NULL && next->is_write) {
        next->is_held = true;
        fetch_and_decrement(&next->dependency->num_dependencies);
    }
}

/*
void
LockManager::RemoveRead(struct TxnQueue *queue, struct DependencyInfo *dep) {

    // Splice this element out of the queue
    struct DependencyInfo *next = dep->next;
    struct DependencyInfo *prev = dep->prev;

    // Try to update the previous txn
    if (prev != NULL) {
        assert(prev->next == dep);
        prev->next = next;
    }
    else {
        assert(queue->head == dep);
        queue->head = next;
    }

    // Try to update the next txn
    if (next != NULL) {
        assert(next->prev == dep);
        next->prev = prev;
    }
    else {
        assert(queue->tail == dep);
        queue->tail = prev;
    }
        
    // If the txn that follows is a write, check if it can proceed
    if (next != NULL && prev == NULL && next->is_write) {
        fetch_and_decrement(&next->dependency->num_dependencies);
    }
}
*/

void
LockManager::Kill(Action *txn) {
    struct DependencyInfo *prev;
    struct DependencyInfo *next;
    
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
        struct DependencyInfo *cur = &txn->writeset[i];
        Table<uint64_t, TxnQueue> *tbl = m_tables[cur->record.m_table];
        TxnQueue *value = tbl->GetPtr(cur->record.m_key);
        assert(value != NULL);
        
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        RemoveTxn(value, cur, &prev, &next);

        if (cur->is_held) {
            AdjustWrite(cur);
        }
        else {
            assert(prev != NULL);        
        }
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
    for (size_t i = 0; i < txn->readset.size(); ++i) {
        struct DependencyInfo *cur = &txn->readset[i];
        Table<uint64_t, TxnQueue> *tbl = m_tables[cur->record.m_table];
        TxnQueue *value = tbl->GetPtr(cur->record.m_key);
        assert(value != NULL);
        
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        RemoveTxn(value, cur, &prev, &next);
        if (cur->is_held) {
            AdjustRead(cur);
        }
        else {
            assert(prev != NULL);        
        }
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
}

void
LockManager::Unlock(Action *txn) {
    struct DependencyInfo *prev;
    struct DependencyInfo *next;
    
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->writeset[i].record.m_table];
        TxnQueue *value = tbl->GetPtr(txn->writeset[i].record.m_key);
        assert(value != NULL);

        struct DependencyInfo *dep = &txn->writeset[i];

        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        assert(dep->is_held);
        RemoveTxn(value, dep, &prev, &next);
        assert(prev == NULL);	// A write better be at the front of the queue
        AdjustWrite(dep);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
    for (size_t i = 0; i < txn->readset.size(); ++i) {
        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->readset[i].record.m_table];
        TxnQueue *value = tbl->GetPtr(txn->readset[i].record.m_key);
        assert(value != NULL);

        struct DependencyInfo *dep = &txn->readset[i];
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        assert(dep->is_held);
        RemoveTxn(value, dep, &prev, &next);
        AdjustRead(dep);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
}

bool
LockManager::Lock(Action *txn) {
    txn->num_dependencies = 0;
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
        txn->writeset[i].dependency = txn;
        txn->writeset[i].is_write = true;
        txn->writeset[i].next = NULL;
        txn->writeset[i].prev = NULL;
        txn->writeset[i].is_held = false;

        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->writeset[i].record.m_table];
        TxnQueue *value = 
            tbl->GetPtr(txn->writeset[i].record.m_key);
        assert(value != NULL);

        // Atomically add the transaction to the lock queue and check whether 
        // it managed to successfully acquire the lock
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        AddTxn(value, &txn->writeset[i]);          
        if ((txn->writeset[i].is_held = CheckWrite(value, &txn->writeset[i])) == false) {
            fetch_and_increment(&txn->num_dependencies);
        }
        else {
            assert(txn->writeset[i].prev == NULL);
        }
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        // Unlatch the value
        unlock(&value->lock_word);
    }
      
    for (size_t i = 0; i < txn->readset.size(); ++i) {
        txn->readset[i].dependency = txn;
        txn->readset[i].is_write = false;
        txn->readset[i].is_held = false;

        // We *must* find the key-value pair
        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->readset[i].record.m_table];
        TxnQueue *value = tbl->GetPtr(txn->readset[i].record.m_key);
        assert(value != NULL);
          
        // Atomically add the transaction to the lock queue and check whether 
        // it managed to successfully acquire the lock
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        AddTxn(value, &txn->readset[i]);
        if ((txn->readset[i].is_held = CheckRead(value, &txn->readset[i])) == false) {
            fetch_and_increment(&txn->num_dependencies);
        }        
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
      
    return (txn->num_dependencies == 0);
}
