// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#include <eager_worker.hh>

EagerWorker::EagerWorker(LockManager *mgr, SimpleQueue *input_queue, 
                         SimpleQueue *output_queue, int cpu) {
    m_lock_mgr = mgr;
    m_txn_input_queue = input_queue;
    m_output_queue = output_queue;
    m_cpu_number = cpu;
    m_start_signal = 0;
    m_queue_head = NULL;
    m_queue_tail = NULL;    
}

void
EagerWorker::Run() {
    assert(m_start_signal == 0);    
    
    // Kickstart the worker thread
    pthread_create(&m_worker_thread, NULL, BootstrapWorker, this);

    // Wait for the newly spawned thread to report that it has successfully 
    // initialized
    while (!m_start_signal)
        ;
}

void*
EagerWorker::BootstrapWorker(void *arg) {
    EagerWorker *worker = (EagerWorker*)arg;
    fetch_and_increment(&worker->m_start_signal);
    worker->WorkerFunction();
}

void
EagerWorker::Enqueue(EagerAction *txn) {
    if (m_queue_head == NULL) {
        assert(m_queue_tail == NULL);
        m_queue_head = txn;
        m_queue_tail = txn;
        txn->prev = NULL;
        txn->next = NULL;
    }
    else {
        m_queue_tail->next = txn;
        txn->next = NULL;
        txn->prev = m_queue_tail;
        m_queue_tail = txn;
    }
    assert((m_queue_head == NULL && m_queue_tail == NULL) ||
           (m_queue_head != NULL && m_queue_tail != NULL));
}

void
EagerWorker::RemoveQueue(EagerAction *txn) {
    EagerAction *prev = txn->prev;
    EagerAction *next = txn->next;

    if (m_queue_head == txn) {
        assert(txn->prev == NULL);
        m_queue_head = txn->next;
    }
    else {
        prev->next = next;
    }
    
    if (m_queue_tail == txn) {
        assert(txn->next == NULL);
        m_queue_tail = txn->prev;
    }
    else {
        next->prev = prev;
    }
    assert((m_queue_head == NULL && m_queue_tail == NULL) ||
           (m_queue_head != NULL && m_queue_tail != NULL));
}

bool
EagerWorker::CheckReady(EagerAction **to_proc) {
    for (EagerAction *iter = m_queue_head; iter != NULL; iter = iter->next) {
        if (iter->num_dependencies == 0) {
            RemoveQueue(iter);
            *to_proc = iter;
            return true;
        }
    }
    return false;
}

void
EagerWorker::TryExec(EagerAction *txn) {
    if (m_lock_mgr->Lock(txn)) {
        assert(txn->num_dependencies == 0);
        txn->Execute();
        m_lock_mgr->Unlock(txn);
        txn->PostExec();

        EagerAction *link;
        if (txn->IsLinked(&link)) {
            TryExec(link);
        }
        m_output_queue->EnqueueBlocking((uint64_t)txn);
    }
    else {
        Enqueue(txn);
    }
}

void
EagerWorker::DoExec(EagerAction *txn) {
    assert(txn->num_dependencies == 0);
    txn->Execute();
    m_lock_mgr->Unlock(txn);
    txn->PostExec();
    
    EagerAction *link;
    if (txn->IsLinked(&link)) {
        TryExec(link);
    }
    m_output_queue->EnqueueBlocking((uint64_t)txn);
}

void
EagerWorker::WorkerFunction() {
    EagerAction *txn;
    while (true) {
        while (CheckReady(&txn)) {
            DoExec(txn);
        }
        if (m_txn_input_queue->Dequeue((uint64_t*)&txn)) {
            TryExec(txn);
        }
    }
}
