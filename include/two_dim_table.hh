#ifndef TWO_DIM_TABLE_HH_
#define TWO_DIM_TABLE_HH_

template<class V>
class TwoDimTable : public Table<uint64_t, V> {

private:
    uint32_t 	m_dim1;
    uint32_t 	m_dim2;
    V 			**m_table;
    uint32_t 	(*m_access1) (uint64_t composite1);
    uint32_t 	(*m_access2) (uint64_t composite2);

public:
    TwoDimTable(uint32_t dim1, uint32_t dim2, 
                uint32_t (*access1) (uint64_t composite1), 
                uint32_t (*access2) (uint64_t composite2)) {
        m_table = (V**)malloc(sizeof(V*)*dim1);
        for (uint32_t i = 0; i < dim1; ++i) {
            m_table[i] = (V*)malloc(sizeof(V)*dim2);
            for (uint32_t j = 0; j < dim2; ++j) {
                m_table[i][j] = V();
            }
        }
        m_dim1 = dim1;
        m_dim2 = dim2;
        m_access1 = access1;
        m_access2 = access2;
    }
    
    virtual V*
    Put(uint64_t key, V value) {
        uint32_t index1 = m_access1(key);
        uint32_t index2 = m_access2(key);
        assert(index1 < m_dim1);
        assert(index2 < m_dim2);
        m_table[index1][index2] = value;
        return &m_table[index1][index2];
    }

    virtual V
    Get(uint64_t key) {
        uint32_t index1 = m_access1(key);
        uint32_t index2 = m_access2(key);
        assert(index1 < m_dim1);
        assert(index2 < m_dim2);
        return m_table[index1][index2];
    }

    virtual V*
    GetPtr(uint64_t key) {
        uint32_t index1 = m_access1(key);
        uint32_t index2 = m_access2(key);
        if (index1 >= m_dim1 || index2 >= m_dim2) {
            std::cout << index1 << " " << m_dim1 << " " << index2 << " " <<  m_dim2 << "\n";
        }
        assert(index1 < m_dim1);
        assert(index2 < m_dim2);
        return &m_table[index1][index2];
    }
  
    virtual V
    Delete(uint64_t key) {
        assert(false);
    }    
};

#endif
