#ifndef 	ONE_DIM_TABLE_HH_
#define 	ONE_DIM_TABLE_HH_

#include <table.hh>
#include <cstdlib>

template<class V>
class OneDimTable : public Table<uint64_t, V> {
private:
    uint32_t 			m_dim;
    V					*m_values;

public:
    OneDimTable(uint32_t dim) {
        m_dim = dim;
        m_values = (V*)malloc(sizeof(V)*dim);
        for (uint32_t i = 0; i < dim; ++i) {
            m_values[i] = V();
        }
    }

    virtual V*
    Put(uint64_t key, V value) {
        assert(key < m_dim);
        m_values[key] = value;
        return &m_values[key];
    }

    virtual V
    Get(uint64_t key) {
        assert(key < m_dim);
        return m_values[key];
    }
    
    virtual V*
    GetPtr(uint64_t key) {
        assert(key < m_dim);
        return &m_values[key]; 
    }
    
    virtual V
    Delete(uint64_t key) {
        assert(key < m_dim);
        return m_values[key];
    }
};

#endif  // 	ONE_DIM_TABLE_HH_
