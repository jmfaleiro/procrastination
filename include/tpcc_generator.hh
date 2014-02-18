#ifndef TPCC_GENERATOR_HH_
#define TPCC_GENERATOR_HH_

#include <workload_generator.h>
#include <tpcc.hh>
#include <iostream>

class TPCCGenerator : public WorkloadGenerator {
private:
    TPCCUtil m_util;
    
public:
    TPCCGenerator() {
        std::cout << "Num items: " << s_num_items << "\n";
    }

    Action*
    genNext() {
        int pct = m_util.gen_rand_range(1, 100);
        return gen_payment();
        if (pct <= 45) {
            return gen_new_order();
        }
        else if (pct <= 88) {
            return gen_payment();
        }
        else {
            int swtch_rand = m_util.gen_rand_range(1, 3);
            switch (swtch_rand) {
            case 1:
                return gen_stock_level();
            case 2:
                return gen_delivery();
            case 3:
                return gen_order_status();
            }
        }
        assert(false);
    }
    
    NewOrderTxn*
    gen_new_order() {
        uint64_t w_id = (uint64_t)m_util.gen_rand_range(0, s_num_warehouses-1);
        uint64_t d_id = (uint64_t)m_util.gen_rand_range(0, s_districts_per_wh-1);
        uint64_t c_id = (uint64_t)m_util.gen_rand_range(0, s_customers_per_dist-1);
        
        uint32_t num_items = 5 + rand() % 11;
        uint64_t *item_ids = (uint64_t*)malloc(sizeof(uint64_t)*num_items);
        assert(item_ids != NULL);
        uint64_t *supplier_wh_ids = 
            (uint64_t*)malloc(sizeof(uint64_t)*num_items);
        assert(supplier_wh_ids != NULL);
        uint32_t *quantities = (uint32_t*)malloc(sizeof(uint32_t)*num_items);
        int all_local = 1;

		for (uint32_t i = 0; i < num_items; i++) {
            item_ids[i] = (uint64_t)m_util.gen_rand_range(0, s_num_items-1);
            assert(item_ids[i] < s_num_items);
            int pct = m_util.gen_rand_range(0, 99);
            if (pct > 1) {
                supplier_wh_ids[i] = w_id;
            }
            else {
                do {
                    supplier_wh_ids[i] = m_util.gen_rand_range(0, s_num_warehouses-1);
                } while (supplier_wh_ids[i] == w_id && s_num_warehouses > 1);
                all_local = 0;
            }
		}

        // 1% of NewOrder transactions should abort.
        if (rand() % 100 == 1) {
            item_ids[num_items-1] = NewOrderTxn::invalid_item_key;
        }
        
        for (uint32_t i = 0; i < num_items; ++i) {
            assert(item_ids[i] < s_num_items || 
                   item_ids[i] == NewOrderTxn::invalid_item_key);
        }
        NewOrderTxn *ret = new NewOrderTxn(w_id, d_id, c_id, 
                                                       all_local, num_items, 
                                                       item_ids, 
                                                       supplier_wh_ids, 
                                                       quantities);
        assert(ret->writeset.size() == num_items+1);
        assert(ret->readset.size() == num_items+2);
        for (uint32_t i = 1; i < num_items+1; ++i) {
            assert(ret->writeset[i].record.m_table == STOCK);
        }
        for (uint32_t i = 2; i < num_items; ++i) {
            assert(ret->readset[i].record.m_table == ITEM);
            assert(ret->readset[i].record.m_key < s_num_items);
        }
        ret->materialize = true;
        return ret;
    }
    
    PaymentTxn*
    gen_payment() {
        uint32_t warehouse_id = 
            (uint32_t)m_util.gen_rand_range(0, s_num_warehouses-1);
        uint32_t district_id = 
            (uint32_t)m_util.gen_rand_range(0, s_districts_per_wh-1);
        uint32_t customer_id = 
            (uint32_t)m_util.gen_customer_id();
        int x = m_util.gen_rand_range(1, 100);
        uint32_t customer_d_id;
        uint32_t customer_w_id;

        if (x <= 85) {
            customer_d_id = district_id;
            customer_w_id = warehouse_id;
        }
        else {
            customer_d_id = (uint32_t)m_util.gen_rand_range(0, s_districts_per_wh-1);
            do {
                customer_w_id = 
                    (uint32_t)m_util.gen_rand_range(0, s_num_warehouses-1);
            } while (customer_w_id == warehouse_id);
        }
        
        float payment_amt = (float)m_util.gen_rand_range(100, 500000)/100.0;
        return new PaymentTxn(warehouse_id, customer_w_id, payment_amt, 
                              district_id, customer_d_id, customer_id, NULL, 
                              false);
    }

    StockLevelTxn*
    gen_stock_level() {
        uint32_t warehouse_id = m_util.gen_rand_range(0, s_num_warehouses);
        uint32_t district_id = m_util.gen_rand_range(0, s_districts_per_wh);
        int threshold = 1000;
        return new StockLevelTxn(warehouse_id, district_id, threshold);
    }
    
    DeliveryTxn*
    gen_delivery() {
        uint32_t warehouse_id = m_util.gen_rand_range(0, s_num_warehouses);
        uint32_t district_id = m_util.gen_rand_range(0, s_districts_per_wh);
        return new DeliveryTxn(warehouse_id, district_id, 0);
    }

    OrderStatusTxn*
    gen_order_status() {
        uint32_t warehouse_id = m_util.gen_rand_range(0, s_num_warehouses);
        uint32_t district_id = m_util.gen_ran_range(0, s_districts_per_wh);
        uint32_t customer_id = (uint32_t)m_util.gen_customer_id();
        return new OrderStatusTxn(warehouse_id, district_id, customer_id, NULL, 
                                  false);        
    }
};

#endif // TPCC_GENERATOR_HH_
