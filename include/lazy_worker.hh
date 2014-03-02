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
    Action 				*m_action;
    ActionNode			*m_next;
    ActionNode			*m_left;
    ActionNode			*m_right;
};

class LazyWorker : public Runnable {
private:
    SimpleQueue 			*m_input_queue;			// Txns to process
    SimpleQueue 			*m_output_queue;		// Dump finished txns here
    SimpleQueue 			*m_feedback_queue;		
    uint64_t				m_cpu_number;			// CPU to which to bind
    
    // List of pending closures    
    ActionNode 				*m_pending_head;
    ActionNode 				*m_pending_tail;
    
    // This list shadows the pending closure list
    ActionNode 				*m_waiting_head;	
    ActionNode 				*m_waiting_tail;

    // Number of elements still to process
    uint32_t 				m_num_elems;		

    void
    CheckList(ActionNode *list);

    void
    EnqueueInner(ActionNode *list, ActionNode **head, ActionNode **tail);

    void
    DequeueInner(ActionNode *node, ActionNode **head, ActionNode **tail);

    void
    Enqueue(ActionNode *proc_node, ActionNode *wait_node);

    void
    Dequeue(ActionNode *proc_node, ActionNode *wait_node);

    ActionNode				*m_free_list;		// Free-list of ActionNodes

    void
    InitActionNodes();
    
    // Get an action node from the free-list
    inline ActionNode*
    GetActionNode();
    
    // Return a list of action nodes to the free-list
    inline void
    ReturnActionNodes(ActionNode *head, ActionNode *tail);

    static void
    ListAppend(ActionNode **lst_head, ActionNode **lst_tail, ActionNode *head);
           

    void
    processWrite(Action *action, int writeIndex, ActionNode **p_head, 
                 ActionNode **p_tail, ActionNode **w_head, ActionNode **w_tail);
    
    void
    processRead(Action *action, int writeIndex, ActionNode **p_head, 
                ActionNode **p_tail, ActionNode **w_head, ActionNode **w_tail);
    
    void
    ProcessTxn(Action *txn, ActionNode **proc_head, ActionNode **proc_tail, 
               ActionNode **wait_head, ActionNode **wait_tail);
    
    void
    RunClosure(ActionNode *to_proc, ActionNode **tail);

    void
    CheckReady();

    bool
    CheckDependencies(ActionNode *waits, ActionNode **tail);

protected:    
    void
    StartWorking();
    
public:
    LazyWorker(SimpleQueue *input_queue, SimpleQueue *feedback_queue, 
               SimpleQueue *output_queue, int cpu);
    
};

#endif 		//  LAZY_WORKER_HH_
