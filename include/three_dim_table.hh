#ifndef THREE_DIM_TABLE_HH_
#define THREE_DIM_TABLE_HH_

#include <table.hh>
#include <numa.h>

template<class V>
class ThreeDimTable : public Table<uint64_t, V> {

private:
    uint32_t 	m_dim1;
    uint32_t 	m_dim2;
    uint32_t 	m_dim3;
    V 			***m_table;
    uint32_t 	(*m_access1) (uint64_t composite1);
    uint32_t 	(*m_access2) (uint64_t composite2);
    uint32_t 	(*m_access3) (uint64_t composite3);

public:
    ThreeDimTable(uint32_t dim1, uint32_t dim2, uint32_t dim3,
                uint32_t (*access1) (uint64_t composite1), 
                uint32_t (*access2) (uint64_t composite2),
                uint32_t (*access3) (uint64_t composite3)) {
        numa_set_strict(1);
        m_table = (V***)numa_alloc_local(sizeof(V)*dim1);
        for (uint32_t i = 0; i < dim1; ++i) {
            m_table[i] = (V**)numa_alloc_local(sizeof(V*)*dim2);
            for (uint32_t j = 0; j < dim2; ++j) {
                m_table[i][j] = (V*)numa_alloc_local(sizeof(V)*dim3);
                for (uint32_t k = 0; k < dim3; ++k) {
                    m_table[i][j][k] = V();
                }
            }
        }
        m_dim1 = dim1;
        m_dim2 = dim2;
        m_dim3 = dim3;
        m_access1 = access1;
        m_access2 = access2;
        m_access3 = access3;
    }
    
    virtual V*
    Put(uint64_t key, V value) {
        uint32_t index1 = m_access1(key);
        uint32_t index2 = m_access2(key);
        uint32_t index3 = m_access3(key);
        assert(index1 < m_dim1);
        assert(index2 < m_dim2);
        assert(index3 < m_dim3);
        m_table[index1][index2][index3] = value;
        return &m_table[index1][index2][index3];
    }

    virtual V
    Get(uint64_t key) {
        uint32_t index1 = m_access1(key);
        uint32_t index2 = m_access2(key);
        uint32_t index3 = m_access3(key);
        assert(index1 < m_dim1);
        assert(index2 < m_dim2);
        assert(index3 < m_dim3);
        return m_table[index1][index2][index3];
    }

    virtual V*
    GetPtr(uint64_t key) {
        uint32_t index1 = m_access1(key);
        uint32_t index2 = m_access2(key);
        uint32_t index3 = m_access3(key);
        assert(index1 < m_dim1);
        assert(index2 < m_dim2);
        assert(index3 < m_dim3);
        return &m_table[index1][index2][index3];
    }
  
    virtual V
    Delete(uint64_t key) {
        assert(false);
    }    
};

#endif
