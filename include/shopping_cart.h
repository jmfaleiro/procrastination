#ifndef SHOPPING_CART_H
#define SHOPPING_CART_H

#include "workload_generator.h"
#include <iostream>
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

  std::vector<int> m_client_indices;
  std::set<int> m_index_set;

 public:
  ShoppingCart(int num_clients, 
	       int num_records, 
	       int cart_size, 
	       int freq, 
	       int hot) {
    
    std::cout << "Shopping cart!\n";
    m_hot = hot;
    m_num_clients = num_clients;
    m_num_records = num_records;
    m_cart_size = 40;
    m_freq = freq;
    m_next_client = 0;
    


    int last_client_index = 0;
    for (int i = 0; i < m_num_clients; ++i) {
      m_client_indices.push_back(last_client_index);
      m_index_set.insert(last_client_index);

      last_client_index += m_num_records / m_num_clients;
    }
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
    int cur_client = m_client_indices[m_next_client % m_num_clients];
    m_next_client += 1;

    Action* action = &m_action_set[m_use_next++];    
    action->start_time = 0;
    action->end_time = 0;
    action->system_start_time = 0;
    action->system_end_time = 0;

    // Both regular updates and check-outs write the shopping cart itself.
    struct DependencyInfo fake;
    fake.record.m_table = 0;
    fake.record.m_key = cur_client;
    action->writeset.push_back(fake);
    action->is_blind = 0;
    action->materialize = false;

    // Check-out if the cart size is now 20. 
    if (cart->size() == 20) {
      
      // Make sure that the action will be materialized. 
      action->materialize = true;

      // Flip a coin, if we like the result, generate a blind-write.
      if (rand() % m_freq == 0) {
	action->is_blind = 1;
      }
      // Regular checkout. 
      else {

	// Make sure that the check-out writes the records in the cart. 
	struct DependencyInfo blah;
	for (std::set<int>::iterator it = cart->begin();
	     it != cart->end();
	     ++it) {
          blah.record.m_table = 0;
	  blah.record.m_key = *it;
	  action->writeset.push_back(blah);
	}
      }
      
      // In either case, make sure that the client-side shopping cart state  is
      // cleared. 
      cart->clear();
    }
    
    // Otherwise keep adding to the cart. 
    else {	
      struct DependencyInfo temp;
      int record;

      //      for (int i = 0; i < 20; ++i) {
	
	// Pick a random item that we want to buy, mark it as a read. 
	while (true) {
	  record = rand() % m_num_records;
	  if (cart->find(record) == cart->end() && 
	      m_index_set.find(record) == m_index_set.end()) {
	    break;
	  }
	}
        temp.record.m_table = 0;
	temp.record.m_key = record;
	action->readset.push_back(temp);
	cart->insert(record);
      }
    //    }
    //    assert(!action->is_blind || action->materialize);
    /*
    if (!action->materialize) {
      assert(action->readset.size() == 1);
      assert(action->writeset.size() == 1);      
    }
    else if (action->is_blind) {
      assert(action->readset.size() == 0);
      assert(action->writeset.size() == 1);
    }
    else {
      if (action->writeset.size() != 21) {
	std::cout << action->writeset.size() << "\n";
      }
      assert(action->writeset.size() == 21);
      assert(action->readset.size() == 0);
      }*/
    return action;
  }
};


#endif // SHOPPING_CART_H
