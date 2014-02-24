// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#ifndef 		EAGER_WORKER_HH_
#define 		EAGER_WORKER_HH_

#include <lock_manager.hh>
#include <concurrent_queue.h>
#include <pthread.h>
#include <util.h>

class EagerWorker {
private:
    LockManager			*m_lock_mgr;			// Global lock manager
    SimpleQueue 		*m_txn_input_queue;		// Thread-local input queue
    SimpleQueue 		*m_output_queue;		// Thread-local output queue
    int 				m_cpu_number;			// CPU to which to bind
    volatile uint64_t	m_start_signal;			// Flag indicating we've begun
    pthread_t 			m_worker_thread;		// Worker thread
    EagerAction 		*m_queue_head;			// Head of queue of waiting txns
    EagerAction			*m_queue_tail;			// Tail of queue of waiting txns

    // The worker threads starts executing in this function
    static void*
    BootstrapWorker(void *arg);

    // Worker thread function
    virtual void
    WorkerFunction();

    void
    Enqueue(EagerAction *txn);

    void
    RemoveQueue(EagerAction *txn);

    bool
    CheckReady(EagerAction **to_proc);

    void
    TryExec(EagerAction *txn);

    void
    DoExec(EagerAction *txn);

public:
    EagerWorker(LockManager *mgr, SimpleQueue *input_queue, 
                SimpleQueue *output_queue, int cpu);
    
    // Should be called from the co-ordinating function
    //
    // Spawns a thread dedicated to processing txns pinned to a particular cpu 
    // (as specified by m_cpu_number).
    virtual void
    Run();    

};

#endif 		 // EAGER_WORKER_HH_
