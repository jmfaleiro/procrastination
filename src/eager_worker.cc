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
EagerWorker::WorkerFunction() {
    while (true) {
        /*
        EagerAction *txn = (EagerAction*)m_txn_input_queue->DequeueBlocking();
        if (m_lock_mgr->Lock(txn)) {
            txn->Execute();
        }
        else {
            
        }
        m_lock_mgr->Unlock(txn);
        txn->PostExec();
        m_output_queue->EnqueueBlocking((uint64_t)txn);
        */
    }
}
