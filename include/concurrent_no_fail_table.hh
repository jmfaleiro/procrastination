#ifndef 			CONCURRENT_NO_FAIL_TABLE_HH_
#define 			CONCURRENT_NO_FAIL_TABLE_HH_

#include <concurrent_hash_table.hh>

template<class K, class V>
class ConcurrentNoFailTable : public ConcurrentHashTable<K, V> {
    
public:
    ConcurrentNoFailTable(uint32_t size, uint32_t chain_bound, 
                          uint64_t (*hash)(K key) = NULL) 
        : ConcurrentHashTable<K, V>(size, chain_bound, hash) { }
    
    virtual V*
    GetPtr(K key) {
        BucketItem<K, V> *bucket = GetBucket(key);        

        if (bucket == NULL) {
            uint64_t index = this->m_hash_function(key) & this->m_mask;
            BucketItem<K, V> *to_insert = new BucketItem<K, V>(key, V());    

            BucketItem<K, V> **table = this->m_table;
            uint64_t *lock_word = 
                (uint64_t*)((char*)table+CACHE_LINE*index+
                            sizeof(BucketItem<K, V>*));
            lock(lock_word);
            BucketItem<K, V> **head = 
                (BucketItem<K, V>**)((char*)table+CACHE_LINE*index);        
            BucketItem<K, V> *to_ret = *head;
            while (to_ret != NULL && to_ret->m_key != key) {
                to_ret = to_ret->m_next;
            }
            if (to_ret == NULL) {
                to_insert->m_next = *head;
                *head = to_insert;
            }
            unlock(lock_word);            
            if (to_ret != NULL) {
                delete(to_insert);
            }
            else {
                to_ret = to_insert;
            }
            return &to_ret->m_value;
        }

        if (bucket == NULL) {
            return NULL;
        }
        else {
            return &bucket->m_value;
        }
    }

};


#endif 			//  CONCURRENT_NO_FAIL_TABLE_HH_
