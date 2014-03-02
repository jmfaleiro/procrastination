// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#include <lazy_worker.hh>

LazyWorker::LazyWorker(SimpleQueue *input_queue, SimpleQueue *feedback_queue, 
                       SimpleQueue *output_queue, int cpu) 
    : Runnable(cpu) {
    m_input_queue = input_queue;
    m_output_queue = output_queue;
    m_cpu_number = (uint64_t)cpu;
    InitActionNodes();
}

void
LazyWorker::InitActionNodes() {
    size_t num_nodes = 1<<24;
    size_t size = sizeof(ActionNode)*num_nodes;
    m_free_list = (ActionNode*)malloc(size);
    memset(m_free_list, 0, size);

    for (size_t i = 0; i < num_nodes-1; ++i) {
        m_free_list[i].m_next = &m_free_list[i+1];
    }
    m_free_list[num_nodes-1].m_next = NULL;
}

void
LazyWorker::ListAppend(ActionNode **lst_head, ActionNode **lst_tail, 
                       ActionNode *head) {                       
    assert((*lst_head == NULL && *lst_tail == NULL) || 
           (*lst_head != NULL && *lst_tail != NULL));    
    assert(head != NULL);
    assert(head->m_action != NULL);
    head->m_next = NULL;

    if (*lst_head == NULL) {
        *lst_head = head;
        *lst_tail = head;
    }
    else {
        (*lst_tail)->m_next = head;
        *lst_tail = head;
    }
}

// Returns two values: 1. The transitive closure of txns to process 
// 						(proc_head, proc_tail)
//
// 					   2. The set of txns we need to wait for
//						(wait_head, wait_tail)
void
LazyWorker::ProcessTxn(Action *txn, ActionNode **proc_head, 
                       ActionNode **proc_tail, ActionNode **wait_head, 
                       ActionNode **wait_tail) {
    assert(txn != NULL);
    assert(txn->state == PROCESSING);
    assert(txn->owner == m_cpu_number);

    // If we've reached this point, we have permission to run the txn
    int num_reads = txn->readset.size();
    int num_writes = txn->writeset.size();
    for (int i = 0; i < num_reads; ++i) {
        processRead(txn, i, proc_head, proc_tail, wait_head, wait_tail);
    }
    
    // Process all the elements in the write set
    for (int i = 0; i < num_writes; ++i) {
        processWrite(txn, i, proc_head, proc_tail, wait_head, wait_tail);
    }
  
    // Check if it is safe to run the dependencies of this txn
    ActionNode *cur_node = GetActionNode();
    cur_node->m_action = txn;    
    ListAppend(proc_head, proc_tail, cur_node);
}

void
LazyWorker::processWrite(Action *action, int writeIndex, ActionNode **p_head,
                         ActionNode **p_tail, ActionNode **w_head, 
                         ActionNode **w_tail) {
    
    // Get the record and txn this action is dependent on. 
    Action* prev = action->writeset[writeIndex].dependency;
    bool is_write = action->writeset[writeIndex].is_write;
    int index = action->writeset[writeIndex].index;

    while (prev != NULL) {
        
        // We only care if this txn has not been substantiated
        if (prev->state != SUBSTANTIATED) {
            if (cmp_and_swap(&prev->state, STICKY, PROCESSING)) {
                // The dependency is our responsibility
                xchgq(&prev->owner, m_cpu_number);
                ProcessTxn(prev, p_head, p_tail, w_head, w_tail);
            }
            else { 
                assert(prev->owner != m_cpu_number);
                // Someone's taken ownership of this dependency
                ActionNode *to_wait = GetActionNode();
                to_wait->m_action = prev;
                ListAppend(w_head, w_tail, to_wait);
            }
        }
        
        if (is_write) {
            break;
        }        
        int cur_index = index;
        is_write = prev->readset[cur_index].is_write;
        index = prev->readset[cur_index].index;
        prev = prev->readset[cur_index].dependency;        
    }    
}

void
LazyWorker::processRead(Action *action, int readIndex, ActionNode **p_head, 
                         ActionNode **p_tail, ActionNode **w_head, 
                         ActionNode **w_tail) {

    Action* prev = action->readset[readIndex].dependency;
    int index = action->readset[readIndex].index;
    bool is_write = action->readset[readIndex].is_write;
    
    bool ret = false;
    
    while (prev != NULL) {
        if (is_write) {
            int state;
            
            if (prev->state == SUBSTANTIATED) {
                // The dependency is already processed, nothing to do here
                return;
            }
            else if (cmp_and_swap(&prev->state, STICKY, PROCESSING)) {
                // The dependency is our responsibility
                xchgq(&prev->owner, m_cpu_number);
                ProcessTxn(prev, p_head, p_tail, w_head, w_tail);
                return;
            }            
            else {
                assert(prev->state == PROCESSING);
                assert(prev->owner != m_cpu_number);
                ActionNode *to_wait = GetActionNode();
                to_wait->m_action = prev;
                ListAppend(w_head, w_tail, to_wait);
            }

            // We can end since this is a write. 
            prev = NULL;
        }
        else {  
            if (prev->state == SUBSTANTIATED) {
                return;
            }
            else {
                int cur_index = index;
                is_write = prev->readset[cur_index].is_write;
                index = prev->readset[cur_index].index;
                prev = prev->readset[cur_index].dependency;
            }
        }
    }    
}

void
LazyWorker::CheckList(ActionNode *list) {
    while (list != NULL) {
        assert(list->m_action != NULL);
        list = list->m_next;
    }
}

void
LazyWorker::EnqueueInner(ActionNode *list, ActionNode **head, ActionNode **tail) {
    //    assert(list->m_left == NULL);
    //    assert(list->m_right == NULL);
    CheckList(list);

    if (*head == NULL) {
        assert(*tail == NULL);
        list->m_left = NULL;
        list->m_right = NULL;
        *head = list;
        *tail = list;
    }
    else {
        assert(*tail != NULL);
        assert((*tail)->m_right == NULL);
        
        (*tail)->m_right = list;
        list->m_left = (*tail);
        list->m_right = NULL;
        *tail = list;
    }
    assert((*tail)->m_right == NULL && (*head)->m_left == NULL);
}

void
LazyWorker::DequeueInner(ActionNode *node, ActionNode **head, ActionNode **tail) {
    assert(node != NULL && head != NULL && tail != NULL);
    assert(*head != NULL && *tail != NULL);
    assert((*tail)->m_right == NULL && (*head)->m_left == NULL);

    ActionNode *left = node->m_left;
    ActionNode *right = node->m_right;
    
    // We're at the head of the queue
    if (left == NULL) {
        assert(*head == node);
        *head = right;
    }
    else {
        assert(*head != node);
        assert(left->m_right == node);
        left->m_right = right;
    }
    
    // We're at the tail of the queue
    if (right == NULL) {
        assert(*tail == node);
        *tail = left;
    }
    else {
        assert(*tail != node);
        assert(right->m_left == node);
        right->m_left = left;
    }
    node->m_left = NULL;
    node->m_right = NULL;
    
    assert((*tail == NULL) || ((*tail)->m_right == NULL && (*head)->m_left == NULL));
}

void
LazyWorker::Enqueue(ActionNode *proc_node, ActionNode *wait_node) {
    assert((m_pending_head == NULL && m_pending_tail == NULL) ||
           (m_pending_head != NULL && m_pending_tail != NULL));
    assert((m_waiting_head == NULL && m_waiting_tail == NULL) ||
           (m_waiting_head != NULL && m_waiting_tail != NULL));
    assert(proc_node != NULL && wait_node != NULL);
    assert((m_pending_head == NULL && m_waiting_head == NULL) ||
           (m_pending_head != NULL && m_waiting_head != NULL));

    EnqueueInner(proc_node, &m_pending_head, &m_pending_tail);
    EnqueueInner(wait_node, &m_waiting_head, &m_waiting_tail);
    m_num_elems += 1;
}

void
LazyWorker::Dequeue(ActionNode *proc_node, ActionNode *wait_node) {
    DequeueInner(proc_node, &m_pending_head, &m_pending_tail);
    DequeueInner(wait_node, &m_waiting_head, &m_waiting_tail);
    m_num_elems -= 1;
}

bool
LazyWorker::CheckDependencies(ActionNode *waits, ActionNode **tail) {
    ActionNode *ret = NULL;
    for (; waits != NULL && waits->m_action->state == SUBSTANTIATED; 
         waits = waits->m_next) {
        ret = waits;
    }
    *tail = ret;
    return (waits == NULL);
}

// Goes through the list of pending transitive closures and tries to execute 
// them
void
LazyWorker::CheckReady() {
    ActionNode *pending = m_pending_head;
    ActionNode *waiting = m_waiting_head;

    while (pending != NULL) {
        assert(waiting != NULL);
        CheckList(waiting);			// XXX: REMOVE ME
        CheckList(pending);			// XXX: REMOVE ME
        
        ActionNode *run_head, *wait_head;
        run_head = pending;
        wait_head = waiting;

        pending = pending->m_right;
        waiting = waiting->m_right;

        ActionNode *w_tail, *p_tail;
        if (CheckDependencies(wait_head, &w_tail)) {
            RunClosure(run_head, &p_tail);
            Dequeue(run_head, wait_head);
            ReturnActionNodes(run_head, p_tail);
            ReturnActionNodes(wait_head, w_tail);
        }        
    }
}

// Runs the entire transitive closure
void
LazyWorker::RunClosure(ActionNode *to_proc, ActionNode **tail) {
    assert(to_proc != NULL);

    ActionNode *iter1 = to_proc, *iter2 = to_proc;
    while (iter1 != NULL) {
        Action *action = iter1->m_action;
        if (action->state == PROCESSING) {
            action->LaterPhase();
        }
        iter1 = iter1->m_next;
    }
    iter1 = NULL;
    while (iter2 != NULL) {
        xchgq(&iter2->m_action->state, SUBSTANTIATED);
        Action *continuation = NULL;
        if (iter2->m_action->IsLinked(&continuation)) {
            m_feedback_queue->Enqueue((uint64_t)continuation);
        }
        else {
            m_output_queue->Enqueue((uint64_t)iter2);
        }
        iter1 = iter2;
        iter2 = iter2->m_next;
    }
    *tail = iter1;
}

void
LazyWorker::StartWorking() {
    Action *txn;
    while (true) {
        if ((m_num_elems < 1000) && (m_input_queue->Dequeue((uint64_t*)&txn))) {
            ActionNode *proc_head = NULL; 
            ActionNode *proc_tail = NULL; 
            ActionNode *wait_head = NULL;
            ActionNode *wait_tail = NULL;

            ProcessTxn(txn, &proc_head, &proc_tail, &wait_head, &wait_tail);
            assert(proc_head != NULL);
            assert(proc_tail->m_action == txn);

            // If there are no dependencies to wait for, we can execute the 
            // closure immediately
            if (wait_head == NULL) {
                ActionNode *temp;
                RunClosure(proc_head, &temp);
                assert(temp == proc_tail);
                ReturnActionNodes(proc_head, proc_tail);
            }
            else { // Defer closure execution
                Enqueue(proc_head, wait_head);
            }
        }
        else { 
            CheckReady();
        }
    }
}

// Return an ActionNode from the free-list of action nodes
ActionNode*
LazyWorker::GetActionNode() {
    assert(m_free_list != NULL);
    ActionNode *ret = m_free_list;
    m_free_list = m_free_list->m_next;
    ret->m_next = NULL;
    ret->m_left = NULL;
    ret->m_right = NULL;
    ret->m_action = NULL;
    return ret;
}

// Return a list of ActionNodes to the free-list
void
LazyWorker::ReturnActionNodes(ActionNode *head, ActionNode *tail) {
    tail->m_next = m_free_list;
    m_free_list = head;
}
