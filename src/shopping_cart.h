#ifndef SHOPPING_CART_H
#define SHOPPING_CART_H

#include "workload_generator.h"
#include <numa.h>
#include <set>

class ShoppingCart : public WorkloadGenerator {

 private:
  int m_num_clients;
  int m_num_records;
  int m_cart_size;
  int m_freq;
  std::set<int>** m_carts;
  int m_next_client;
  int m_hot;

 public:
  ShoppingCart(int num_clients, 
	       int num_records, 
	       int cart_size, 
	       int freq, 
	       int hot) {

    m_hot = hot;
    m_num_clients = num_clients;
    m_num_records = num_records;
    m_cart_size = cart_size;
    m_freq = freq;
    m_next_client = 0;

    m_carts = new std::set<int>*[m_num_clients];
    for (int i = 0; i < m_num_clients; ++i) {
      m_carts[i] = new std::set<int>();
    }

    m_num_actions = 10000000;
    m_action_set = new Action[m_num_actions];
    memset(m_action_set, 0, sizeof(Action)*m_num_actions);
    m_use_next = 0;
    srand(time(NULL));
  }
  
  Action* genNext() {
    std::set<int>* cart = m_carts[m_next_client % m_num_clients];
    int cur_client = m_next_client % m_num_clients;
    m_next_client += 1;

    Action* action = &m_action_set[m_use_next++];    
    
    if (rand() % m_cart_size == 0) {

      struct DependencyInfo blah;
      blah.record = cur_client;
      action->writeset.push_back(blah);

      for (std::set<int>::iterator it = cart->begin();
	   it != cart->end();
	   ++it) {
	struct DependencyInfo inf;
	inf.record = *it;
	action->writeset.push_back(inf);	
      }
      
      action->materialize = true;
      action->is_blind = ((rand() % m_freq) == 0);
      cart->clear();
    }
    else {
      struct DependencyInfo real, fake;
      int record;

      // Pick a hot record. 
      if (rand() % 2 == 0) {
	while (true) {
	  record = rand() % m_hot;
	  if ((cart->find(record) == cart->end()) && record >= m_num_clients) {
	    break;
	  }
	}
      }
      // Pick a cold record. 
      else {	
	while (true) {
	  record = rand() % m_num_records;
	  if ((cart->find(record) == cart->end()) && 
	      (record >= m_num_clients) &&
	      (record >= m_hot)) {
	    break;
	  }
	}
      }
      
      cart->insert(record);
      real.record = record;
      fake.record = cur_client;
      action->writeset.push_back(real);
      action->writeset.push_back(fake);

      action->materialize = false;      
      action->is_blind = false;
    }
    return action;
  }  
};


#endif // SHOPPING_CART_H
