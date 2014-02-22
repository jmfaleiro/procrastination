// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#include <lock_manager.hh>

using namespace cc_params;

LockManager::LockManager(TableInit *params, int num_params) {
    m_tables = do_tbl_init<TxnQueue>(params, num_params);
}

bool
LockManager::CheckWrite(struct TxnQueue *queue, struct DependencyInfo *dep) {
    return queue->head->dependency == dep->dependency;
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
    if (queue->tail != NULL) {
        assert(queue->head != NULL);
        dep->prev = queue->tail;
        *(queue->tail) = dep;
        queue->tail = &dep->next;
        dep->next = NULL;
    }
    else {
        assert(queue->head == NULL);
        queue->head = dep;
        queue->tail = &dep->next;
        dep->prev = NULL;
        dep->next = NULL;
    }
}

void
LockManager::RemoveWrite(struct TxnQueue *queue, struct DependencyInfo *dep) {
       
    // This txn was a write, it better be at the front of the queue
    assert(queue->head == dep);
    assert(dep->prev == NULL);
    queue->head = dep->next;
        
    // If the queue is empty, make sure the tail reflects it
    if (dep->next == NULL) {
        assert(*(queue->tail) == dep);
        queue->tail == NULL;
    }
    else {
        dep->next->prev = NULL;
        if (dep->next->is_write) {
            fetch_and_decrement(&dep->next->dependency->num_dependencies);
        }
        else {
            for (struct DependencyInfo *iter = dep->next; 
                 iter != NULL && !iter->is_write; iter = iter->next) {
                fetch_and_decrement(&iter->dependency->num_dependencies);
            }
        }
    } 
}

void
LockManager::RemoveRead(struct TxnQueue *queue, struct DependencyInfo *dep) {

    // Splice this element out of the queue
    struct DependencyInfo *next = dep->next;
    struct DependencyInfo **prev = dep->prev;

    // Try to update the previous txn
    if (prev != NULL) {
        assert((*prev)->next == dep);
        (*prev)->next = next;
    }
    else {
        assert(queue->head == dep);
        queue->head = dep->next;            
    }

    // Try to update the next txn
    if (next != NULL) {
        assert(*(next->prev) == dep);
        next->prev = prev;
    }
    else {
        assert(*(queue->tail) == dep);
        queue->tail = prev;
    }
        
    // If the txn that follows is a write, check if it can proceed
    if (next != NULL && prev == NULL && next->is_write) {
        fetch_and_decrement(&next->dependency->num_dependencies);
    }
}


void
LockManager::Unlock(Action *txn) {
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->writeset[i].record.m_table];
        TxnQueue *value = tbl->GetPtr(txn->writeset[i].record.m_key);
        assert(value != NULL);
        
        lock(&value->lock_word);
        
        unlock(&value->lock_word);
    }
    for (size_t i = 0; i < txn->readset.size(); ++i) {

    }
}

bool
LockManager::Lock(Action *txn) {
      for (size_t i = 0; i < txn->writeset.size(); ++i) {
          txn->writeset[i].dependency = txn;
          txn->writeset[i].is_write = true;

          Table<uint64_t, TxnQueue> *tbl = 
              m_tables[txn->writeset[i].record.m_table];
          TxnQueue *value = 
              tbl->GetPtr(txn->writeset[i].record.m_key);
          assert(value != NULL);

          // Atomically add the transaction to the lock queue and check whether 
          // it managed to successfully acquire the lock
          lock(&value->lock_word);
          AddTxn(value, &txn->writeset[i]);          
          if (!CheckWrite(value, &txn->writeset[i])) {
              fetch_and_increment(&txn->num_dependencies);
          }

          // Unlatch the value
          unlock(&value->lock_word);
      }
      
      for (size_t i = 0; i < txn->readset.size(); ++i) {
          txn->readset[i].dependency = txn;
          txn->readset[i].is_write = false;

          // We *must* find the key-value pair
          Table<uint64_t, TxnQueue> *tbl = 
              m_tables[txn->writeset[i].record.m_table];
          TxnQueue *value = tbl->GetPtr(txn->readset[i].record.m_key);
          assert(value != NULL);
          
          // Atomically add the transaction to the lock queue and check whether 
          // it managed to successfully acquire the lock
          lock(&value->lock_word);
          AddTxn(value, &txn->readset[i]);
          if (!CheckRead(value, &txn->readset[i])) {
              fetch_and_increment(&txn->num_dependencies);
          }
          unlock(&value->lock_word);
      }
      
      return (txn->num_dependencies == 0);
}
