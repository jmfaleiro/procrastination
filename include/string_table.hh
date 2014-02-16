// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#ifndef STRING_TABLE_HH_
#define STRING_TABLE_HH_

#include <stdlib.h>

template<class V>
class StringTable : public HashTable<char*, V> {
    static uint64_t
    string_hash_function(char *str) {
        size_t len = strlen(str);
        return CityHash64(str, len);
    }
    
public:
    StringTable(uint32_t size, uint32_t chain_bound) 
        : HashTable<char *, V>(size, chain_bound, 
                               malloc(sizeof(BucketItem<char*, V>*)*size)) {
        memset(this->m_table, 0, sizeof(BucketItem<char*, V>*)*this->m_size);
    }

    virtual void
    Put(char* key, V value) {
        uint64_t index = string_hash_function(key) & (this->m_size - 1);
        BucketItem<char*, V> *to_insert = new BucketItem<char*, V>(key, value);
        BucketItem<char*, V> **table = this->m_table;        
        to_insert->m_next = table[index];
        table[index] = to_insert;
    }
    
    virtual V
    Get(char *key) {
        uint64_t index = string_hash_function(key) & (this->m_size - 1);
        BucketItem<char*, V> *iter = this->m_table[index];
        while (iter != NULL && strcmp(key, iter->m_key) != 0) {
            iter = iter->m_next;
        }
        if (iter == NULL) {
            return V();
        }
        else {
            return iter->m_value;
        }
    }
    
    virtual V*
    GetPtr(char *key) {
        assert(false);
        return NULL;
    }
    
    virtual V
    Delete(char* key) {
        assert(false);
        return V();
    }
};

#endif // STRING_TABLE_HH_
