// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#ifndef 	CONCURRENCY_CONTROL_PARAMS_HH_
#define 	CONCURRENCY_CONTROL_PARAMS_HH_

#include <cassert>
#include <iostream>
#include <table.hh>
#include <hash_table.hh>
#include <bulk_allocating_table.hh>
#include <concurrent_hash_table.hh>
#include <two_dim_table.hh>
#include <three_dim_table.hh>
#include <one_dim_table.hh>

using namespace std;

namespace cc_params {
    enum TableType {
        HASH_TABLE = 0,
        CONCURRENT_HASH_TABLE,
        BULK_ALLOCATING_TABLE,
        ONE_DIM_TABLE,
        TWO_DIM_TABLE,
        THREE_DIM_TABLE,
        NONE,
    };

    struct HashTableInit {
        uint32_t 		m_size;
        uint32_t 		m_chain_bound;
    };

    struct ConcurrentHashTableInit {
        uint32_t 		m_size;
        uint32_t 		m_chain_bound;
    };

    struct BulkAllocatingTableInit {
        uint32_t 		m_size;
        uint32_t 		m_chain_bound;
        uint32_t 		m_allocation_size;
    };

    struct OneDimTableInit {
        uint32_t 		m_dim1;
    };

    struct TwoDimTableInit {
        uint32_t 		m_dim1;
        uint32_t 		m_dim2;
        uint32_t		(*m_access1) (uint64_t composite);
        uint32_t 		(*m_access2) (uint64_t composite);
    };

    struct ThreeDimTableInit {
        uint32_t 		m_dim1;
        uint32_t 		m_dim2;
        uint32_t 		m_dim3;
        uint32_t		(*m_access1) (uint64_t composite);
        uint32_t 		(*m_access2) (uint64_t composite);
        uint32_t 		(*m_access3) (uint64_t composite);    
    };

    typedef union {
        struct HashTableInit 					m_hash_params;
        struct ConcurrentHashTableInit			m_conc_params;
        struct BulkAllocatingTableInit			m_bulk_params;
        struct OneDimTableInit					m_one_params;
        struct TwoDimTableInit					m_two_params;
        struct ThreeDimTableInit				m_three_params;
    } TableParams;

    typedef struct {
        TableType	   	    m_table_type;
        TableParams			m_params;
    } TableInit;

    template<class V>
    static Table<uint64_t, V>**
    do_tbl_init(TableInit *tbl_init, int num_params) {
        Table<uint64_t, V> **ret = 
            (Table<uint64_t, V>**)malloc(sizeof(Table<uint64_t, V>*)*
                                         num_params);
        memset(ret, 0, sizeof(Table<uint64_t, V>*)*num_params);
        for (int i = 0; i < num_params; ++i) {
            TableType table_type = tbl_init[i].m_table_type;
            TableParams table_params = tbl_init[i].m_params;
            switch (table_type) {
            case HASH_TABLE:
                ret[i] = 
                    new HashTable<uint64_t, V>
                    (table_params.m_hash_params.m_size, 
                     table_params.m_hash_params.m_chain_bound);
                break;
            case CONCURRENT_HASH_TABLE:
                ret[i] =
                    new ConcurrentHashTable<uint64_t, V>
                    (table_params.m_conc_params.m_size,
                     table_params.m_conc_params.m_chain_bound);
                break;
            case BULK_ALLOCATING_TABLE:
                ret[i] = new BulkAllocatingTable<uint64_t, V>
                    (table_params.m_bulk_params.m_size,
                     table_params.m_bulk_params.m_chain_bound,
                     table_params.m_bulk_params.m_allocation_size);
                break;
            case ONE_DIM_TABLE:
                ret[i] = 
                    new OneDimTable<V>
                    (table_params.m_one_params.m_dim1);
                break;
            case TWO_DIM_TABLE:
                ret[i] = new TwoDimTable<V>
                    (table_params.m_two_params.m_dim1,
                     table_params.m_two_params.m_dim2,
                     table_params.m_two_params.m_access1,
                     table_params.m_two_params.m_access2);
                    
                break;
            case THREE_DIM_TABLE:
                ret[i] = new ThreeDimTable<V>
                    (table_params.m_three_params.m_dim1,
                     table_params.m_three_params.m_dim2,
                     table_params.m_three_params.m_dim3,
                     table_params.m_three_params.m_access1,
                     table_params.m_three_params.m_access2,
                     table_params.m_three_params.m_access3);
                break;
            case NONE:
                ret[i] = NULL;
                break;
            default:
                cout << "concurrent_control_params.hh: ";
                cout << "Got an unexpected table type!\n";
                assert(false);
                break;
            }
        }
        return ret;
    }
};

#endif // 	CONCURRENCY_CONTROL_PARAMS_HH_
