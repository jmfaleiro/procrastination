// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#ifndef HASH_TABLE_HH_
#define HASH_TABLE_HH_

#define POWER_TWO(x) ((x) & ((x) - 1) == 0)

#include <cassert>

#include "city.h"

template <class K, class V>
class BucketItem {
public:
  BucketItem<K, V> *m_next;
  K m_key;
  V m_value;

  BucketItem(K key, V value) {
    m_key = key;
    m_value = value;
    m_next = NULL;
  }
  ~BucketItem() {}
};

template <class K, class V>
class HashTable {
protected:
  uint32_t m_size;
  uint32_t m_mask;

  // Make sure that chains don't get too large. 
  uint32_t m_chain_bound;       
  BucketItem<K, V> **m_table;

  static inline uint64_t
  hash_function(K key) {
    char *start = (char*)&key;
    return CityHash64(start, sizeof(K));
  }

public:
  HashTable(uint32_t size, uint32_t chain_bound) {
    assert(!(size & (size - 1)));
    m_size = size;
    m_mask = size - 1;
    m_chain_bound = chain_bound;

#ifndef NDEBUG
    m_table = (BucketItem<K, V>**)malloc(sizeof(BucketItem<K, V>*)*m_size*2);
    memset(m_table, 0, 2*size);
#else
    m_table = (BucketItem<K, V>**)malloc(sizeof(BucketItem<K,V>*)*m_size);
    memset(m_table, 0, size);
#endif
  }

  virtual void
  Put(K key, V value) {
    uint64_t index = hash_function(key) & m_mask;
    BucketItem<K, V> *to_insert = new BucketItem<K, V> (key, value);

#ifndef NDEBUG
    to_insert->m_next = m_table[2*index];
    m_table[2*index] = to_insert;

    // Ensure that the chain length is reasonable. 
    uint64_t *counter_ptr = (uint64_t*)&m_table[2*index + 1];
    *counter_ptr += 1;
    assert(*counter_ptr <= m_chain_bound);
#else
    to_insert->m_next = m_table[index];
    m_table[index] = to_insert;    
#endif
  }
  
  virtual V
  Get(K key) {
    uint64_t index = hash_function(key) & m_mask;

#ifndef NDEBUG
    BucketItem<K, V> *to_ret = m_table[2*index];
#else
    BucketItem<K, V> *to_ret = m_table[index];
#endif

    while (to_ret != NULL && to_ret->m_key != key) {
      to_ret = to_ret->m_next;
    }
    if (to_ret == NULL) {
      return V();
    }
    else {
      return to_ret->m_value;
    }
  }

  virtual V
  Delete(K key) {
    uint64_t index = hash_function(key) & m_mask;

#ifndef NDEBUG
    BucketItem<K,V> **prev = &m_table[2*index];
    BucketItem<K,V> *to_remove = m_table[2*index];
#else    
    BucketItem<K,V> **prev = &m_table[index];
    BucketItem<K,V> *to_remove = m_table[index];
#endif
    
    while (to_remove->m_key != key) {
      prev = &((*prev)->m_next);
      to_remove = to_remove->m_next;
    }
    *prev = to_remove->m_next;
    V ret = to_remove->m_value;
    delete(to_remove);

#ifndef NDEBUG
    uint64_t *counter_ptr = (uint64_t *)&m_table[2*index + 1];
    *counter_ptr -= 1;
#endif
    return ret;
  }  
};

#endif // HASH_TABLE_HH_
