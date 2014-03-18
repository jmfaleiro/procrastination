// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#include <eager_worker.hh>

EagerWorker::EagerWorker(LockManager *mgr, SimpleQueue *input_queue, 
                         SimpleQueue *output_queue, int cpu) 
    : Runnable(cpu) {
    m_lock_mgr = mgr;
    m_txn_input_queue = input_queue;
    m_output_queue = output_queue;
    m_cpu_number = cpu;
    m_queue_head = NULL;
    m_queue_tail = NULL;    
    m_num_elems = 0;
    m_num_done = 0;
}

void
EagerWorker::StartWorking() {
    WorkerFunction();
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
    m_num_elems += 1;
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
    
    m_num_elems -= 1;
    assert(m_num_elems >= 0);
    assert((m_queue_head == NULL && m_queue_tail == NULL) ||
           (m_queue_head != NULL && m_queue_tail != NULL));
}

uint32_t
EagerWorker::QueueCount(EagerAction *iter) {
    if (iter == NULL) {
        return 0;
    }
    else {
        return 1+QueueCount(iter->next);
    }
}

void
EagerWorker::CheckReady() {
    for (EagerAction *iter = m_queue_head; iter != NULL; iter = iter->next) {
        if (iter->num_dependencies == 0) {
            RemoveQueue(iter);
            DoExec(iter);
        }
    }
}

void
EagerWorker::TryExec(EagerAction *txn) {
    if (m_lock_mgr->Lock(txn)) {
        assert(txn->num_dependencies == 0);
        txn->Execute();
        m_lock_mgr->Unlock(txn);

        assert(txn->finished_execution);
        txn->PostExec();

        EagerAction *link;
        if (txn->IsLinked(&link)) {
            TryExec(link);
        }
        else {
            m_num_done += 1;
            clock_gettime(CLOCK_REALTIME, &txn->end_time);
            //            txn->end_rdtsc_time = rdtsc();
            m_output_queue->EnqueueBlocking((uint64_t)txn);
        }
    }    
    else {
        m_num_done += 1;
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
    else {
        m_num_done += 1;
        clock_gettime(CLOCK_REALTIME, &txn->end_time);
        //        txn->end_rdtsc_time = rdtsc();
        m_output_queue->EnqueueBlocking((uint64_t)txn);
    }
}

void
EagerWorker::WorkerFunction() {
    EagerAction *txn;
    uint32_t i = 0;

    while (true) {        
        CheckReady();
        if (m_num_elems < 10 && m_txn_input_queue->Dequeue((uint64_t*)&txn)) {
            clock_gettime(CLOCK_REALTIME, &txn->start_time);
            //            txn->start_rdtsc_time = rdtsc();
            TryExec(txn);
        }
    }
}
