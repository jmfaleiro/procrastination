#ifndef TPCC_GENERATOR_HH_
#define TPCC_GENERATOR_HH_

#include <workload_generator.h>
#include <tpcc.h>

class TPCCGenerator : public WorkloadGenerator {
private:
    uint32_t m_num_warehouses;
    uint32_t m_dist_per_wh;
    uint32_t m_cust_per_dist;
    uint32_t m_item_count;
    
public:
    TPCCGenerator(uint32_t num_warehouses, uint32_t dist_per_wh, 
                  uint32_t cust_per_dist, uint32_t item_count) {
        m_num_warehouses = num_warehouses;
        m_dist_per_wh = dist_per_wh;
        m_cust_per_dist = cust_per_dist;
        m_item_count = item_count;
    }

    Action*
    genNext() {
        uint64_t w_id = 1+rand() % m_num_warehouses;
        uint64_t d_id = 1+rand() % m_dist_per_wh;
        uint64_t c_id = 1+rand() % m_cust_per_dist;
        
        uint32_t num_items = 5 + rand() % 11;
        uint64_t *item_ids = (uint64_t*)malloc(sizeof(uint64_t)*num_items);
        uint64_t *supplier_wh_ids = 
            (uint64_t*)malloc(sizeof(uint64_t)*num_items);
        uint32_t *quantities = (uint32_t*)malloc(sizeof(uint32_t)*num_items);
        int all_local = 1;

		for (uint32_t i = 0; i < num_items; i++) {
            item_ids[i] = rand() % m_item_count;
            if (rand() % 100 > 1) {
                supplier_wh_ids[i] = w_id;
            }
            else {
                do {
                    supplier_wh_ids[i] = 1 + rand() % m_num_warehouses;
                } while (supplier_wh_ids[i] == w_id && m_num_warehouses > 1);
                all_local = 0;
            }
		}

        // 1% of NewOrder transactions should abort.
        if (rand() % 100 == 1) {
            item_ids[num_items-1] = tpcc::NewOrderTxn::invalid_item_key;
        }
        
        tpcc::NewOrderTxn *ret = new tpcc::NewOrderTxn(w_id, d_id, c_id, 
                                                       all_local, num_items, 
                                                       item_ids, 
                                                       supplier_wh_ids, 
                                                       quantities);
        return ret;
    }    
};

#endif // TPCC_GENERATOR_HH_
