// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#ifndef BULK_ALLOCATING_TABLE_HH_
#define BULK_ALLOCATING_TABLE_HH_

#include <hash_table.hh>

// BulkAllocatingTable is not thread-safe!!! 
//
// Built specifically to get around bottlenecks in the memory allocator. The 
// basic idea is to allocate BucketItems in bulk and put them into a free-list. 
// When we need a new BucketItem, we take it from the free-list.
//
// XXX: No support for deletion (yet). Should be pretty straightforward using 
// doubly-linked BucketItems.
//
template<class K, class V>
class BulkAllocatingTable : public HashTable<K, V> {
private:
    BucketItem<K, V>		*m_free_list;
    uint32_t				m_allocation_size;

protected:
    virtual BucketItem<K, V>*
    PutInternal(K key, V value) {
        uint64_t index = (this->m_hash_function(key)) & (this->m_mask);
        if (m_free_list == NULL) {
            ExpandFreeList();
        }
        BucketItem<K, V> *to_insert = m_free_list;
        m_free_list = m_free_list->m_next;

        to_insert->m_next = this->m_table[index];
        to_insert->m_key = key;
        to_insert->m_value = value;
        this->m_table[index] = to_insert;        
        return to_insert;
    }

public:
    BulkAllocatingTable(uint32_t size, uint32_t chain_bound, 
                        uint32_t allocation_size, 
                        uint64_t (*hash)(K key) = NULL)
        : HashTable<K, V> (size, 
                           chain_bound, 
                           malloc(sizeof(BucketItem<K, V>*)*size),
                           hash) {
        memset(this->m_table, 0, size);
        assert(this->m_hash_function != hash || 
               this->m_hash_function == this->default_hash_function);
        
        m_free_list = NULL;
        m_allocation_size = allocation_size;
        ExpandFreeList();
    }

    virtual void
    ExpandFreeList() {
        std::cout << "warning: Performing bulk allocation!!!\n";
        BucketItem<K, V> *head = 
            (BucketItem<K, V>*)malloc(sizeof(BucketItem<K, V>)*
                                      m_allocation_size);
        BucketItem<K, V> *temp = head;
        for (uint32_t i = 0; i < m_allocation_size-1; ++i) {
            temp->m_next = &head[i+1];
            temp = temp->m_next;
        }
        temp->m_next = m_free_list;
        m_free_list = head;
    }
};

#endif // BULK_ALLOCATING_TABLE_HH_
