// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#ifndef 		LAZY_WORKER_HH_
#define 		LAZY_WORKER_HH_

#include <action.h>
#include <concurrent_queue.h>
#include <util.h>
#include <runnable.hh>

enum ActionState {
    STICKY,
    PROCESSING,
    SUBSTANTIATED,
};

class ActionNode {
public:
    Action 				*action;
    ActionNode 			*prev;
    ActionNode 			*next;
};

class LazyWorker : public Runnable {
private:
    SimpleQueue 			*m_input_queue;			// Txns to process
    SimpleQueue 			*m_output_queue;		// Dump finished txns here
    SimpleQueue 			*m_feedback_queue;		
    uint64_t				m_cpu_number;			// CPU to which to bind
    
    // List of pending txns    
    ActionNode 				*m_queue_head;
    ActionNode 				*m_queue_tail;
    long	 				m_num_elems;		

    void
    CheckWaits();

    void
    Enqueue(ActionNode *txn);

    void
    RemoveQueue(ActionNode *txn);

    ActionNode				*m_free_list;		// Free-list of ActionNodes

    void
    InitActionNodes();
    
    // Get an action node from the free-list
    inline ActionNode*
    GetActionNode();
    
    // Return a list of action nodes to the free-list
    inline void
    ReturnActionNode(ActionNode *node);

    bool
    ProcessTxn(Action *txn);

    bool
    processRead(Action *action, int readIndex);

    bool
    processWrite(struct DependencyInfo *info);
    
    bool
    ProcessFunction(Action *txn);

protected:    
    void
    StartWorking();
    
public:
    LazyWorker(SimpleQueue *input_queue, SimpleQueue *feedback_queue, 
               SimpleQueue *output_queue, int cpu);
    
};

#endif 		//  LAZY_WORKER_HH_
