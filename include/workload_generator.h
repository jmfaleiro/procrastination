#ifndef WORKLOAD_GENERATOR_H
#define WORKLOAD_GENERATOR_H

#include "action.h"
#include <cstring>
#include <iostream>
#include <cassert>

class WorkloadGenerator {
 protected:
    int m_pre_gen_index;
	Action* m_action_set;
    int m_num_actions;
	int m_use_next;
	int m_blind_write_freq;

 public:

    // Generate a random action. 
    virtual Action* genNext() = 0;
  
    virtual int numUsed() {
        return m_use_next;
    }
  
    virtual Action* getActions() {
        return m_action_set;
    }

    // Generate a set of transactions to run. Do this so that transaction
    // generation does not get into the critical path of a client. 
    virtual void preGen() {
        m_pre_gen_index = 0;
        std::cout << "Begin gen...\n";
        for (int i = 0; i < m_num_actions; ++i) {
            genNext();
        }
        std::cout << "End gen...\n";
    }

    // Get the next pre-generated action to process. 
    virtual Action* getPreGen() {
        assert(m_pre_gen_index < m_num_actions);
        Action* ret = &m_action_set[m_pre_gen_index];
        m_pre_gen_index += 1;
        return ret;
    }
};

#endif // WORKLOAD_GENERATOR_H
