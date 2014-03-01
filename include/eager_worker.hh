// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#ifndef 		EAGER_WORKER_HH_
#define 		EAGER_WORKER_HH_

#include <lock_manager.hh>
#include <concurrent_queue.h>
#include <pthread.h>
#include <cpuinfo.h>
#include <runnable.hh>

class EagerWorker : public Runnable {
private:
    LockManager			*m_lock_mgr;			// Global lock manager
    SimpleQueue 		*m_txn_input_queue;		// Thread-local input queue
    SimpleQueue 		*m_output_queue;		// Thread-local output queue
    int 				m_cpu_number;			// CPU to which to bind
    EagerAction 		*m_queue_head;			// Head of queue of waiting txns
    EagerAction			*m_queue_tail;			// Tail of queue of waiting txns
    int					m_num_elems;			// Number of elements in the queue
    uint32_t 			m_num_done;
    
    // Worker thread function
    virtual void
    WorkerFunction();

    void
    Enqueue(EagerAction *txn);

    void
    RemoveQueue(EagerAction *txn);

    void
    CheckReady();

    void
    TryExec(EagerAction *txn);

    void
    DoExec(EagerAction *txn);
    
    uint32_t
    QueueCount(EagerAction *txn);

protected:    
    virtual void
    StartWorking();

public:
    EagerWorker(LockManager *mgr, SimpleQueue *input_queue, 
                SimpleQueue *output_queue, int cpu);
};

#endif 		 // EAGER_WORKER_HH_
