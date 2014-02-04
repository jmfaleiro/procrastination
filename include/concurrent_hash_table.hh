// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#ifndef CONCURRENT_HASH_TABLE_HH_
#define CONCURRENT_HASH_TABLE_HH_

#include "util.h"
#include "machine.h"
#include "hash_table.hh"
#include "city.h"

template <class K, class V>
class ConcurrentHashTable : public HashTable<K, V> {

  // The layout of an entry in the values table at 8-byte granularity.
  // 
  //    data     chain     lock    u   u   u   u   u
  // |---------|--------|--------|---|---|---|---|---|
  //
  // *u means that the 8-byte word is unused. 

public:
  ConcurrentHashTable(uint32_t size, uint32_t chain_bound) 
    : HashTable<K, V>(size, 
		chain_bound, 
		malloc(sizeof(BucketItem<K, V>*)*size*CACHE_LINE)) {
    memset(this->m_table, 0, sizeof(BucketItem<K, V>*)*this->m_size*CACHE_LINE);
  }

  virtual void
  Put(K key, V value) {
    uint64_t index = this->hash_function(key) & this->m_mask;
    BucketItem<K, V> *to_insert = new BucketItem<K, V>(key, value);
    BucketItem<K, V> **table = this->m_table;


    uint64_t *lock_word = (uint64_t*)&table[CACHE_LINE*index+2];
    lock(lock_word);
    
    // Insert the item into the chain. 
    to_insert->m_next = table[CACHE_LINE*index];
    table[CACHE_LINE*index] = to_insert;

    // Ensure that the chain length is reasonable. 
#ifndef NDEBUG
    uint64_t *counter_ptr = (uint64_t*)&table[2*index + 1];
    *counter_ptr += 1;
    assert(*counter_ptr <= m_chain_bound);
#endif

    unlock(lock_word);
  }
  
  virtual V
  Get(K key) {
    uint64_t index = this->hash_function(key) & this->m_mask;    
    V ret;

    BucketItem<K, V> **table = this->m_table;
    uint64_t *lock_word = (uint64_t*)&table[CACHE_LINE*index+2];
    lock(lock_word);

    BucketItem<K, V> *to_ret = table[CACHE_LINE*index];
    while (to_ret != NULL && to_ret->m_key != key) {
      to_ret = to_ret->m_next;
    }
    if (to_ret == NULL) {
      ret = V();
    }
    else {
      ret = to_ret->m_value;
    }
    unlock(lock_word);
    return ret;
  }
  
  virtual V
  Delete(K key) {
    uint64_t index = this->hash_function(key) & this->m_mask;
    BucketItem<K, V> **table = this->m_table;

    // Grab the chain lock. 
    uint64_t *lock_word = (uint64_t*)&table[CACHE_LINE*index+2];
    lock(lock_word);

    
    BucketItem<K,V> **prev = &table[CACHE_LINE*index];
    BucketItem<K,V> *to_remove = table[CACHE_LINE*index];
    
    while (to_remove->m_key != key) {
      prev = &((*prev)->m_next);
      to_remove = to_remove->m_next;
    }
    *prev = to_remove->m_next;
#ifndef NDEBUG
    uint64_t *counter_ptr = (uint64_t *)&table[*index + 1];
    *counter_ptr -= 1;
#endif

    // Release the chain lock. 
    unlock(lock_word);

    V ret = to_remove->m_value;
    delete(to_remove);
    return ret;
  }
};

#endif // CONCURRENT_HASH_TABLE_HH_
