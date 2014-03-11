#ifndef SHOPPING_CART_H
#define SHOPPING_CART_H

#include "workload_generator.h"
#include <iostream>
#include <numa.h>
#include <set>
#include <simple_action.hh>
#include <eager_generator.hh>
#include <algorithm>

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
    srand(time(NULL));
  }
  
  Action* genNext() {
    std::set<int>* cart = m_carts[m_next_client % m_num_clients];
    int cur_client = m_next_client % m_num_clients;
    m_next_client += 1;

    // Both regular updates and check-outs write the shopping cart itself.
    struct DependencyInfo fake;
    fake.record.m_table = 0;
    fake.record.m_key = cur_client;

    Action *ret = NULL;

    // Check-out if the cart size is now 20. 
    if (cart->size() == 30) {

      // Flip a coin, if we like the result, generate a blind-write.
      if (rand() % m_freq == 0) {
          uint32_t *to_write = (uint32_t*)malloc(1000);
          for (size_t i = 0; i < 250; ++i) {
              to_write[i] = (uint32_t)rand();
          }
          ret = new shopping::ClearCart(to_write);
          ret->is_blind = true;
          ret->writeset.push_back(fake);
      }
      // Regular checkout. 
      else {
          
          ret = new shopping::Checkout();
          ret->writeset.push_back(fake);

          // Make sure that the check-out writes the records in the cart. 
          struct DependencyInfo blah;
          for (std::set<int>::iterator it = cart->begin();
               it != cart->end();
               ++it) {
              blah.record.m_table = 1;
              blah.record.m_key = *it;
              ret->writeset.push_back(blah);
          }
      }
      
      ret->materialize = true;
      ret->is_blind = false;
      // In either case, make sure that the client-side shopping cart state  is
      // cleared. 
      cart->clear();
    }
    
    // Otherwise keep adding to the cart. 
    else {	
        ret = new shopping::AddItemAction();
        ret->writeset.push_back(fake);
        ret->materialize = false;
        ret->is_blind = false;
        struct DependencyInfo temp;
        int record;

        //        for (int i = 0; i < 20; ++i) {
	
            // Pick a random item that we want to buy, mark it as a read. 
            while (true) {
                record = rand() % m_num_records;
                if (cart->find(record) == cart->end() && 
                    m_index_set.find(record) == m_index_set.end()) {
                    break;
                }
            }

            temp.record.m_table = 1;
            temp.record.m_key = record;
            ret->readset.push_back(temp);
            cart->insert(record);
            //        }
    }
    return ret;
  }
};

class EagerShoppingCart : public EagerGenerator {
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
  EagerShoppingCart(int num_clients, 
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

    srand(time(NULL));
  }
  
  EagerAction* genNext() {
    std::set<int>* cart = m_carts[m_next_client % m_num_clients];
    int cur_client = m_next_client % m_num_clients;
    m_next_client += 1;

    EagerAction *ret = NULL;
    struct EagerRecordInfo fake;
    fake.record.m_key = cur_client;
    fake.record.m_table = 0;

    // Check-out if the cart size is now 20. 
    if (cart->size() == 30) {

      // Flip a coin, if we like the result, generate a blind-write.
      if (rand() % m_freq == 0) {
          uint32_t *to_write = (uint32_t*)malloc(1000);
          for (size_t i = 0; i < 250; ++i) {
              to_write[i] = (uint32_t)rand();
          }
          ret = new shopping::EagerClearCart(to_write);
          ret->writeset.push_back(fake);
      }
      // Regular checkout. 
      else {
          
          ret = new shopping::EagerCheckout();
          ret->writeset.push_back(fake);

          // Make sure that the check-out writes the records in the cart. 
          for (std::set<int>::iterator it = cart->begin();
               it != cart->end();
               ++it) {
              fake.record.m_table = 1;
              fake.record.m_key = *it;
              ret->writeset.push_back(fake);
          }
      }
      
      // In either case, make sure that the client-side shopping cart state  is
      // cleared. 
      cart->clear();
    }
    
    // Otherwise keep adding to the cart. 
    else {	
        ret = new shopping::EagerAddItemAction();
        ret->writeset.push_back(fake);

        int record;

        //        for (int i = 0; i < 20; ++i) {
	
            // Pick a random item that we want to buy, mark it as a read. 
            while (true) {
                record = rand() % m_num_records;
                if (cart->find(record) == cart->end() && 
                    m_index_set.find(record) == m_index_set.end()) {
                    break;
                }
            }
            fake.record.m_table = 1;
            fake.record.m_key = record;
            ret->readset.push_back(fake);
            cart->insert(record);
            //        }
    }
    
    std::sort(ret->readset.begin(), ret->readset.end());
    std::sort(ret->writeset.begin(), ret->writeset.end());
    return ret;
  }
};


#endif // SHOPPING_CART_H
