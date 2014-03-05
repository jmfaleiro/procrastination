// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#include <lazy_worker.hh>
#include <algorithm>

LazyWorker::LazyWorker(SimpleQueue *input_queue, SimpleQueue *feedback_queue, 
                       SimpleQueue *output_queue, int cpu) 
    : Runnable(cpu) {
    m_input_queue = input_queue;
    m_output_queue = output_queue;
    m_feedback_queue = feedback_queue;
    m_cpu_number = (uint64_t)cpu;
    InitActionNodes();
    
    m_queue_head = NULL;
    m_queue_tail = NULL;
    m_num_elems = 0;
}

void
LazyWorker::InitActionNodes() {
    size_t num_nodes = 1<<24;
    size_t size = sizeof(ActionNode)*num_nodes;
    m_free_list = (ActionNode*)malloc(size);
    memset(m_free_list, 0, size);

    for (size_t i = 0; i < num_nodes-1; ++i) {
        m_free_list[i].next = &m_free_list[i+1];
    }
    m_free_list[num_nodes-1].next = NULL;
}

void
LazyWorker::Enqueue(ActionNode *txn) {
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
LazyWorker::RemoveQueue(ActionNode *txn) {
    ActionNode *prev = txn->prev;
    ActionNode *next = txn->next;

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

bool
LazyWorker::ProcessFunction(Action *txn) {
    assert(txn != NULL);
    if (txn->state != SUBSTANTIATED) {
        if (cmp_and_swap(&txn->state, STICKY, PROCESSING)) {
            if (ProcessTxn(txn)) {
                assert(txn->state == SUBSTANTIATED);
                return true;
            }
            else {
                xchgq(&txn->state, STICKY);        
                return false;
            }
        }
        else {	// cmp_and_swap failed
            return false;
        }
    }
    else {	// txn->state == SUBSTANTIATED
        return true;
    }
}

bool
LazyWorker::processWrite(Action *action, int writeIndex) {
    
    // Get the record and txn this action is dependent on. 
    Action* prev = action->writeset[writeIndex].dependency;
    bool is_write = action->writeset[writeIndex].is_write;
    int index = action->writeset[writeIndex].index;

    while (prev != NULL) {
        
        // We only care if this txn has not been substantiated
        if (!ProcessFunction(prev)) {
            return false;
        }
        
        if (is_write) {
            return true;
        }        
        int cur_index = index;
        is_write = prev->readset[cur_index].is_write;
        index = prev->readset[cur_index].index;
        prev = prev->readset[cur_index].dependency;        
    }
    return true;
}

bool
LazyWorker::processRead(Action *action, int readIndex) {
    Action* prev = action->readset[readIndex].dependency;
    int index = action->readset[readIndex].index;
    bool is_write = action->readset[readIndex].is_write;
    
    bool ret = false;
    
    while (prev != NULL) {
        if (is_write) {
            int state;
            
            if (prev->state == SUBSTANTIATED) {
                // The dependency is already processed, nothing to do here
                return true;
            }
            else {
                return ProcessFunction(prev);
            }
        }
        else {  
            if (prev->state == SUBSTANTIATED) {
                return true;
            }
            else {
                int cur_index = index;
                is_write = prev->readset[cur_index].is_write;
                index = prev->readset[cur_index].index;
                prev = prev->readset[cur_index].dependency;
            }
        }
    }
    return true;
}

// Returns two values: 1. The transitive closure of txns to process 
// 						(proc_head, proc_tail)
//
// 					   2. The set of txns we need to wait for
//						(wait_head, wait_tail)
bool
LazyWorker::ProcessTxn(Action *txn) {
    assert(txn != NULL);
    assert(txn->state == PROCESSING);
    
    uint32_t read_index = 0, write_index = 0;
    bool ret = true;
    uint32_t reads_done = 0, writes_done = 0;

    for (size_t i = 0; i < txn->readset.size(); ++i) {
        if (!processRead(txn, i)) {
            ret = false;
        }
    }
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
        if (!processWrite(txn, i)) {
            ret = false;
        }
    }

    if (ret) {
        txn->LaterPhase();
        xchgq(&txn->state, SUBSTANTIATED);
        Action *next_link;
        if (txn->IsLinked(&next_link)) {
            assert(next_link != NULL);
            m_feedback_queue->Enqueue((uint64_t)next_link);
        }
        else {
            m_output_queue->Enqueue((uint64_t)txn);
        }
    }
    return ret;
}

void
LazyWorker::CheckWaits() {
    ActionNode *iter = m_queue_head;
    while (iter != NULL) {
        ActionNode *to_ret = iter;
        iter = iter->next;
        if (ProcessFunction(to_ret->action)) {
            assert(to_ret->action->state == SUBSTANTIATED);
            RemoveQueue(to_ret);
            ReturnActionNode(to_ret);
        }        
    }
}

void
LazyWorker::StartWorking() {
    Action *txn;
    while (true) {

        if (m_num_elems < 1000 && m_input_queue->Dequeue((uint64_t*)&txn)) {
            if (!ProcessFunction(txn)) {
                ActionNode *wait_node = GetActionNode();
                wait_node->action = txn;
                wait_node->prev = NULL;
                wait_node->next = NULL;
                Enqueue(wait_node);
            }
        }
        else {
            CheckWaits();
        }
    }
}

// Return an ActionNode from the free-list of action nodes
ActionNode*
LazyWorker::GetActionNode() {
    assert(m_free_list != NULL);
    ActionNode *ret = m_free_list;
    m_free_list = m_free_list->next;
    ret->next = NULL;
    ret->prev = NULL;
    ret->action = NULL;
    return ret;
}

// Return a list of ActionNodes to the free-list
void
LazyWorker::ReturnActionNode(ActionNode *node) {
    node->next = m_free_list;
    m_free_list = node;
}
