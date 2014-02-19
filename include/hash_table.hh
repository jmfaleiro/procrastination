// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#ifndef HASH_TABLE_HH_
#define HASH_TABLE_HH_

#define POWER_TWO(x) ((x) & ((x) - 1) == 0)

#include <cassert>
#include <stdio.h>
#include <string.h>

#include <table.hh>
#include <city.h>

static uint64_t
string_hash_helper(char *str) {
    size_t len = strlen(str);
    return CityHash64(str, len);
}



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

template<class K, class V>
class TableIterator {
private:
    BucketItem<K, V> *m_cur;
    K m_key;
public:
    TableIterator(BucketItem<K, V> *cur) {
        m_cur = cur;
        m_key = cur->m_key;
    }

    V
    Value() {
        return m_cur->m_value;
    }
    
    void
    Next() {
        do {
            m_cur = m_cur->m_next;
        } while(m_cur != NULL && m_cur->m_key != m_key);
    }
    
    bool
    Done() {
        return m_cur == NULL;
    }
};

template <class K, class V>
class HashTable : public Table<K, V> {
private:

    BucketItem<K, V>*
    PutInternal(K key, V value) {
        uint64_t index = m_hash_function(key) & m_mask;
        BucketItem<K, V> *to_insert = new BucketItem<K, V> (key, value);
        to_insert->m_next = m_table[index];
        m_table[index] = to_insert;    
        return to_insert;
    }

protected:
    uint32_t m_size;
    uint32_t m_mask;

    // Make sure that chains don't get too large. 
    uint32_t m_chain_bound;       
    BucketItem<K, V> **m_table;
    uint64_t (*m_hash_function)(K key);
    
    static uint64_t
    default_hash_function(K key) {
        char *start = (char*)&key;
        return CityHash64(start, sizeof(K));
    }

    HashTable(uint32_t size, uint32_t chain_bound, void *tbl,
              uint64_t (*hash)(K key) = NULL) {
        assert(!(size & (size - 1)));
        m_size = size;
        m_mask = size - 1;
        m_chain_bound = chain_bound;
        
        if (hash == NULL) {
            m_hash_function = default_hash_function;
        }
        else {
            m_hash_function = hash;
        }
        m_table = (BucketItem<K, V>**)tbl;
    }

    virtual BucketItem<K, V>*
    GetBucket(K key) {
        uint64_t index = m_hash_function(key) & m_mask;
        BucketItem<K, V> *to_ret = m_table[index];
        while (to_ret != NULL && to_ret->m_key != key) {
            to_ret = to_ret->m_next;
        }
        return to_ret;
    }

public:
    HashTable(uint32_t size, uint32_t chain_bound, 
              uint64_t (*hash)(K key) = NULL) {
        assert(!(size & (size - 1)));
        m_size = size;
        m_mask = size - 1;
        m_chain_bound = chain_bound;
        
        if (hash == NULL) {
            m_hash_function = default_hash_function;
        }
        else {
            m_hash_function = hash;
        }
        m_table = (BucketItem<K, V>**)malloc(sizeof(BucketItem<K,V>*)*m_size);
        memset(m_table, 0, size);
    }

    virtual V*
    Put(K key, V value) {
        BucketItem<K, V> *temp = PutInternal(key, value);
        return &temp->m_value;
    }
  
    virtual V
    Get(K key) {
        BucketItem<K, V> *bucket = GetBucket(key);
        if (bucket == NULL) {
            return V();
        }
        else {
            return bucket->m_value;
        }
    }
    
    virtual TableIterator<K, V>
    GetIterator(K key) {
        BucketItem<K, V> *bucket = GetBucket(key);
        TableIterator<K, V> ret(bucket);
        return ret;
    }

    virtual V*
    GetPtr(K key) {
        uint64_t index = m_hash_function(key) & m_mask;
        BucketItem<K, V> *item = m_table[index];
        while (item != NULL && item->m_key != key) {
            item = item->m_next;
        }
        if (item == NULL) {
            return NULL;
        }
        else {
            return &item->m_value;
        }
    }

    virtual V
    Delete(K key) {
        uint64_t index = m_hash_function(key) & m_mask;

        BucketItem<K,V> **prev = &m_table[index];
        BucketItem<K,V> *to_remove = m_table[index];
    
        while (to_remove->m_key != key) {
            prev = &((*prev)->m_next);
            to_remove = to_remove->m_next;
        }
        *prev = to_remove->m_next;
        V ret = to_remove->m_value;
        delete(to_remove);

        return ret;
    }  
};

#endif // HASH_TABLE_HH_
