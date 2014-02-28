// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#include <lazy_worker.hh>

LazyWorker::LazyWorker(SimpleQueue *input_queue, SimpleQueue *output_queue, 
                       int cpu) {
    m_input_queue = input_queue;
    m_output_queue = output_queue;
    m_cpu_number = cpu;
}

void
LazyWorker::Run() {
    assert(m_start_signal == 0);
    
    // Kickstart the worker thread
    pthread_create(&m_worker_thread, NULL, BootstrapWorker, this);
    
    // Wait for the newly spawned thread to report that it has successfully
    // initialized
    while (!m_start_signal)
        ;    
}

void*
LazyWorker::BootstrapWorker(void *arg) {
    LazyWorker *worker = (LazyWorker*)arg;
    
    // Pin the thread to a cpu
    if (pin_thread(worker->m_cpu_number) == -1) {
        std::cout << "LazyWorker couldn't bind to a cpu!!!\n";
        exit(-1);
    }

    // Signal that we've initialized
    fetch_and_increment(&worker->m_start_signal);	

	// Start processing input
    worker->WorkerFunction();
}

void
LazyWorker::ListAppend(ActionNode **lst_head, ActionNode **lst_tail, 
                       ActionNode *head, ActionNode *tail) {
    assert((*lst_head == NULL && *lst_tail == NULL) || 
           (*lst_head != NULL && *lst_tail != NULL));
    assert((head == NULL && tail == NULL) || (head != NULL && tail != NULL));
    
    // We only have to append the lists if the second one is non-empty
    if (head != NULL) {
        if (*lst_tail == NULL) {	// The original list is empty
            *lst_head = head;
            *lst_tail = tail;
        }
        else {		// The original list is non-empty
            (*lst_tail)->m_next = head;
            *lst_tail = tail;
        }
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
    
    // Initialize the state we're going to return
    *proc_head = NULL;
    *proc_tail = NULL;
    *wait_head = NULL;
    *wait_tail = NULL;    
    
    // Try to get permission to run this transaction. We get permission if the 
    // its a sticy. Otherwise, someone else has already taken ownership of the 
    // txn.
    int txn_state;
    lock(&txn->lock_word);
    if ((txn_state = txn->state) == STICKY) {
        txn->state = PROCESSING;
    }    
    unlock(&txn->lock_word);

    // We didn't get permission to run this txn
    if (txn_state != STICKY) {
        return;
    }
    
    // If we've reached this point, we have permission to run the txn
    int num_reads = txn->readset.size();
    int num_writes = txn->writeset.size();
    for (int i = 0; i < num_reads; ++i) {
        ActionNode *p_head = NULL, *p_tail = NULL;
        ActionNode *w_head = NULL, *w_tail = NULL;

        processRead(txn, i, &p_head, &p_tail, &w_head, &w_tail);
        ListAppend(proc_head, proc_tail, p_head, p_tail);
        ListAppend(wait_head, wait_tail, w_head, w_tail);
    }
    
    // Process all the elements in the write set
    for (int i = 0; i < num_writes; ++i) {
        ActionNode *p_head = NULL, *p_tail = NULL;
        ActionNode *w_head = NULL, *w_tail = NULL;

        processWrite(txn, i, &p_head, &p_tail, &w_head, &w_tail);
        ListAppend(proc_head, proc_tail, p_head, p_tail);
        ListAppend(wait_head, wait_tail, w_head, w_tail);
    }
  
    // Check if it is safe to run the dependencies of this txn
    ActionNode *cur_node = GetActionNode();
    cur_node->m_action = txn;
    ListAppend(proc_head, proc_tail, cur_node, cur_node);
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
            if (prev->state == PROCESSING) {	
                // Someone's taken ownership of this dependency
                ActionNode *to_wait = GetActionNode();
                to_wait->m_action = prev;
                ListAppend(w_head, w_tail, to_wait, to_wait);
            }
            else if (cmp_and_swap(&prev->state, STICKY, PROCESSING)) {
                // The dependency is our responsibility
                ProcessTxn(prev, p_head, p_tail, w_head, w_tail);            
            }
            else {
                ActionNode *to_wait = GetActionNode();
                to_wait->m_action = prev;
                ListAppend(w_head, w_tail, to_wait, to_wait);
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
    *p_head = NULL;
    *p_tail = NULL;
    *w_head = NULL;
    *w_tail = NULL;

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
            else if (prev->state == PROCESSING) {	
                // Someone's taken ownership of this dependency
                ActionNode *to_wait = GetActionNode();
                to_wait->m_action = prev;
                *w_head = to_wait;
                *w_tail = to_wait;
            }
            else if (cmp_and_swap(&prev->state, STICKY, PROCESSING)) {
                // The dependency is our responsibility
                ProcessTxn(prev, p_head, p_tail, w_head, w_tail);
                return;
            }
            else {
                ActionNode *to_wait = GetActionNode();
                to_wait->m_action = prev;
                *w_head = to_wait;
                *w_tail = to_wait;
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
LazyWorker::Enqueue(ActionNode *proc_node, ActionNode *wait_node) {
    if (m_pending_head == NULL) {
        assert(m_num_elems == 0);
        assert(m_pending_tail == NULL);
        assert(m_waiting_head == NULL && m_waiting_tail == NULL);

        m_pending_head = proc_node;
        m_pending_tail = proc_node;
        proc_node->m_left = NULL;
        proc_node->m_right = NULL;        
        
        m_waiting_head = wait_node;
        m_waiting_tail = wait_node;
        wait_node->m_left = NULL;
        wait_node->m_right = NULL;
    }
    else {
        assert(m_num_elems != 0);
        assert((m_pending_tail != NULL) && (m_pending_tail->m_right == NULL));
        assert((m_waiting_head != NULL) && (m_waiting_tail != NULL) && 
               (m_waiting_tail->m_right == NULL));
        m_pending_tail->m_right = proc_node;
        proc_node->m_left = m_pending_tail;
        proc_node->m_right = NULL;
        m_pending_tail = proc_node;

        m_waiting_tail->m_right = wait_node;
        wait_node->m_left = m_waiting_tail;
        wait_node->m_right = NULL;
        m_waiting_tail = wait_node;
    }
    m_num_elems += 1;
}

void
LazyWorker::Dequeue(ActionNode *proc_node, ActionNode *wait_node) {
    ActionNode *proc_prev = proc_node->m_left;
    ActionNode *proc_next = proc_node->m_right;
    ActionNode *wait_prev = wait_node->m_left;
    ActionNode *wait_next = wait_node->m_right;

    if (m_pending_head == proc_node) {
        assert(m_waiting_head == wait_node);
        assert((proc_node->m_left == NULL) && (wait_node->m_left == NULL));
        
        // Adjust the queue head
        m_pending_head = proc_next;
        m_waiting_head = wait_next;
    }
    else {
        proc_prev->m_right = proc_next;
        wait_prev->m_right = wait_next;
    }
    
    if (m_pending_tail == proc_node) {
        assert(m_waiting_tail == wait_node);
        assert((proc_node->m_right == NULL) && (wait_node->m_right == NULL));
        
        // Adjust the queue tail
        m_pending_tail = proc_prev;
        m_waiting_tail = wait_prev;
    }
    else {
        proc_next->m_left = proc_prev;
        wait_next->m_left = wait_prev;
    }

    m_num_elems -= 1;
}

bool
LazyWorker::CheckDependencies(ActionNode *waits) {
    for (; waits != NULL && waits->m_action->state == SUBSTANTIATED; 
         waits = waits->m_next) 
        ;
    return (waits == NULL);
}

// Goes through the list of pending transitive closures and tries to execute 
// them
void
LazyWorker::CheckReady() {
    for (ActionNode *pending = m_pending_head, *waiting = m_waiting_head;
         pending != NULL; 
         pending = pending->m_right, waiting = waiting->m_right) {
        assert(waiting != NULL);
        if (CheckDependencies(waiting)) {
            RunClosure(pending);
            Dequeue(pending, waiting);
        }
    }
}

// Runs the entire transitive closure
void
LazyWorker::RunClosure(ActionNode *to_proc) {
    ActionNode *iter1 = to_proc, *iter2 = to_proc;
    while (iter1 != NULL) {
        Action *action = iter1->m_action;
        assert(action->state == PROCESSING);
        action->LaterPhase();
        iter1 = iter1->m_next;
    }
    while (iter2 != NULL) {
        lock(&iter2->m_action->lock_word);
        iter2->m_action->state = SUBSTANTIATED;
        unlock(&iter2->m_action->lock_word);
        Action *continuation = NULL;
        if (iter2->m_action->IsLinked(&continuation)) {
            m_feedback_queue->Enqueue((uint64_t)continuation);
        }
        iter2 = iter2->m_next;
    }
}

void
LazyWorker::WorkerFunction() {
    Action *txn;
    while (true) {
        if ((m_num_elems < 1000) && (m_input_queue->Dequeue((uint64_t*)&txn))) {
            ActionNode *proc_head, *proc_tail, *wait_head, *wait_tail;
            ProcessTxn(txn, &proc_head, &proc_tail, &wait_head, &wait_tail);
            
            // If there are no dependencies to wait for, we can execute the 
            // closure immediately
            if (wait_head == NULL) {
                RunClosure(proc_head);
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
    m_free_list = ret->m_next;
    ret->m_next = NULL;
    ret->m_left = NULL;
    ret->m_right = NULL;
    return ret;
}

// Return a list of ActionNodes to the free-list
void
LazyWorker::ReturnActionNodes(ActionNode *head, ActionNode *tail) {
    tail->m_next = m_free_list;
    m_free_list = head;
}
