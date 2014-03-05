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

protected:
    virtual BucketItem<K, V>*
    GetBucket(K key) {
        uint64_t index = this->m_hash_function(key) & this->m_mask;    
        BucketItem<K, V> **table = this->m_table;
        uint64_t *lock_word = 
            (uint64_t*)((char*)table+CACHE_LINE*index+
                        sizeof(BucketItem<K, V>*));
        lock(lock_word);
        BucketItem<K, V> *to_ret = 
            *(BucketItem<K, V>**)((char*)table+CACHE_LINE*index);        
        unlock(lock_word);
        while (to_ret != NULL && to_ret->m_key != key) {
            to_ret = to_ret->m_next;
        }
        return to_ret;
    }

public:
    ConcurrentHashTable(uint32_t size, uint32_t chain_bound, 
                        uint64_t (*hash)(K key) = NULL) 
        : HashTable<K, V>(size, 
                          chain_bound, 
                          malloc(size*CACHE_LINE), 
                          hash) {
        memset(this->m_table, 0, 
               this->m_size*CACHE_LINE);
        assert(this->m_hash_function != hash || 
               this->m_hash_function == this->default_hash_function);
    }

    virtual V*
    Put(K key, V value) {
        uint64_t index = this->m_hash_function(key) & this->m_mask;    
        BucketItem<K, V> *to_insert = new BucketItem<K, V>(key, value);
        BucketItem<K, V> **table = this->m_table;
        uint64_t *lock_word = 
            (uint64_t*)((char*)table+CACHE_LINE*index+
                        sizeof(BucketItem<K, V>*));
        lock(lock_word);
        BucketItem<K, V> **chain_head = 
            (BucketItem<K, V>**)((char*)table+CACHE_LINE*index);        
        to_insert->m_next = *chain_head;
        *chain_head = to_insert;
        unlock(lock_word);
        return &to_insert->m_value;
    }
  
    virtual V
    Get(K key) {
        BucketItem<K, V> *bucket = GetBucket(key);
        return bucket->m_value;
    }

    virtual V*
    GetPtr(K key) {
        BucketItem<K, V> *bucket = GetBucket(key);        
        if (bucket == NULL) {
            return NULL;
        }
        else {
            return &bucket->m_value;
        }
    }
    
    virtual TableIterator<K, V>
    GetIterator(K key) {
        BucketItem<K, V> *bucket = GetBucket(key);
        TableIterator<K, V> ret(bucket);
        return ret;
    }

    virtual V
    Delete(K key) {
        assert(false);
        uint64_t index = this->m_hash_function(key) & this->m_mask;
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
        /*
          #ifndef NDEBUG
          uint64_t *counter_ptr = (uint64_t *)&table[*index + 1];
          *counter_ptr -= 1;
          #endif
        */
        // Release the chain lock. 
        unlock(lock_word);

        V ret = to_remove->m_value;
        delete(to_remove);
        return ret;
    }
};

#endif // CONCURRENT_HASH_TABLE_HH_
