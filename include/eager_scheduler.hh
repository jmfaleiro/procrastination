#ifndef 		EAGER_SCHEDULER_HH_
#define 		EAGER_SCHEDULER_HH_

#include <lock_manager.hh>
#include <concurrent_queue.h>

class EagerScheduler : public Runnable {
private:
    LockManager 		*m_lock_manager;
    SimpleQueue 		*m_input_queue;
    SimpleQueue			**m_worker_input_queues;
    uint32_t 			m_num_workers;

protected:
    
    virtual void
    StartWorking() {
        EagerAction *txn;
        uint64_t index = 0;
        while (true) {
            txn = (EagerAction*)m_input_queue->DequeueBlocking();            
            while (!m_worker_input_queues[index%m_num_workers]->Enqueue((uint64_t)txn)) {
                index += 1;
            }
        }
    }

public:
    EagerScheduler(LockManager *mgr, SimpleQueue *input, SimpleQueue **workers, 
                   uint32_t num_workers, int cpu_number)
        : Runnable(cpu_number) {
        m_lock_manager = mgr;
        m_input_queue = input;
        m_worker_input_queues = workers;
        m_num_workers = num_workers;
    }    
};

#endif 		//  EAGER_SCHEDULER_HH_
