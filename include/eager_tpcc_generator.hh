#ifndef EAGER_TPCC_GENERATOR_HH_
#define EAGER_TPCC_GENERATOR_HH_

#include <tpcc.hh>
#include <eager_tpcc.hh>
#include <iostream>
#include <eager_generator.hh>
#include <set>

using namespace tpcc;

class EagerTPCCGenerator : public EagerGenerator {
private:
    TPCCUtil 		m_util;
    uint32_t 		m_new_order_f;
    uint32_t 		m_payment_f;
    uint32_t 		m_stock_level_f;
    uint32_t 		m_delivery_f;
    uint32_t		m_order_status_f;
    uint32_t 		m_fraction_sum;

    void
    DefaultSplit() {
        m_new_order_f = 45;
        m_payment_f = m_new_order_f + 43;
        m_stock_level_f = m_payment_f + 5;
        m_delivery_f = m_stock_level_f + 5;
        m_order_status_f = m_delivery_f + 5;
        m_fraction_sum = m_order_status_f;
    }
    
public:
    EagerTPCCGenerator() {
        std::cout << "Num items: " << s_num_items << "\n";
        DefaultSplit();
    }
    
    EagerTPCCGenerator(uint32_t new_order, uint32_t payment, 
                       uint32_t stock_level, uint32_t delivery, 
                       uint32_t order_status) {
        std::cout << "Num items: " << s_num_items << "\n";
        m_new_order_f = new_order;
        m_payment_f = m_new_order_f + payment;
        m_stock_level_f = m_payment_f + stock_level;
        m_delivery_f = m_stock_level_f + delivery;
        m_order_status_f = m_delivery_f + order_status;
        m_fraction_sum = m_order_status_f;
        std::cout << "Fraction: " << m_fraction_sum << "\n";
    }

    EagerAction*
    genNext() {
        uint32_t pct = m_util.gen_rand_range(1, m_fraction_sum);
        if (pct <= m_new_order_f) {
            return gen_new_order();
        }
        else if (pct <= m_payment_f) {
            return gen_payment();
        }
        else if (pct <= m_stock_level_f) {
            return gen_stock_level();
        }
        else if (pct <= m_delivery_f) {
            return gen_delivery();
        }
        else {
            return gen_order_status();
        }
    }
    
    NewOrderEager*
    gen_new_order() {
        uint64_t w_id = (uint64_t)m_util.gen_rand_range(0, s_num_warehouses-1); 
        uint64_t d_id = 
            (uint64_t)m_util.gen_rand_range(0, s_districts_per_wh-1);        
        uint64_t c_id = 
            (uint64_t)m_util.gen_rand_range(0, s_customers_per_dist-1);         

        uint32_t num_items = m_util.gen_rand_range(5, 15); 
        uint64_t *item_ids = (uint64_t*)malloc(sizeof(uint64_t)*num_items);
        assert(item_ids != NULL);
        uint64_t *supplier_wh_ids = 
            (uint64_t*)malloc(sizeof(uint64_t)*num_items);
        assert(supplier_wh_ids != NULL);
        uint32_t *quantities = (uint32_t*)malloc(sizeof(uint32_t)*num_items);
        for (uint32_t i = 0; i < num_items; ++i) {
            quantities[i] = m_util.gen_rand_range(1, 10);
        }
        int all_local = 1;

        std::set<uint64_t> seen_items;
		for (uint32_t i = 0; i < num_items; i++) {
            uint64_t cur_item;
            do {
                cur_item = (uint64_t)m_util.gen_rand_range(0, s_num_items-1);
            }
            while (seen_items.find(cur_item) != seen_items.end());            
            seen_items.insert(cur_item);

            item_ids[i] = cur_item;
            assert(item_ids[i] < s_num_items);
            int pct = m_util.gen_rand_range(1, 100);

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
        /*
        if (rand() % 100 == 1) {
            item_ids[num_items-1] = NewOrderEager::invalid_item_key;
        }
        */

        for (uint32_t i = 0; i < num_items; ++i) {
            assert(item_ids[i] < s_num_items || 
                   item_ids[i] == NewOrderEager::invalid_item_key);
        }
        NewOrderEager *ret = new NewOrderEager(w_id, d_id, c_id, 
                                                all_local, num_items, 
                                                item_ids, 
                                                supplier_wh_ids, 
                                                quantities);
        assert(ret->readset[NewOrderEager::s_warehouse_index].record.m_table 
               == WAREHOUSE);
        assert(ret->readset[NewOrderEager::s_customer_index].record.m_table 
               == CUSTOMER);        
        assert(ret->writeset[NewOrderEager::s_district_index].record.m_table 
               == DISTRICT);
        return ret;
    }
    
    PaymentEager*
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

        if (x <= 85 || (s_num_warehouses == 1)) {
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
        return new PaymentEager(warehouse_id, customer_w_id, payment_amt, 
                              district_id, customer_d_id, customer_id, NULL, 
                              false);
    }

    StockLevelEager0*
    gen_stock_level() {
        uint32_t warehouse_id = m_util.gen_rand_range(0, s_num_warehouses-1);
        uint32_t district_id = m_util.gen_rand_range(0, s_districts_per_wh-1);
        int threshold = (int)m_util.gen_rand_range(10, 20);
        StockLevelEager2 *level2 = new StockLevelEager2(warehouse_id, 
                                                        district_id, 
                                                        threshold);
        StockLevelEager1 *level1 = new StockLevelEager1(warehouse_id, 
                                                        district_id, 
                                                        threshold, level2);
        StockLevelEager0 *level0 = new StockLevelEager0(warehouse_id, 
                                                        district_id, 
                                                        threshold, level1, true);
        EagerAction *temp;
        assert(level0->IsLinked(&temp));
        assert(temp == level1);
        assert(level1->IsLinked(&temp));
        assert(temp == level2);
        return level0;
    }
    
    DeliveryEager0*
    gen_delivery() {
        uint32_t warehouse_id = m_util.gen_rand_range(0, s_num_warehouses-1);
        uint32_t district_id = m_util.gen_rand_range(0, s_districts_per_wh-1);

        DeliveryEager2 *level2 = new DeliveryEager2(warehouse_id, district_id, 10);
        DeliveryEager1 *level1 = new DeliveryEager1(warehouse_id, district_id, 10, 
                                                    level2);
        DeliveryEager0 *level0 = new DeliveryEager0(warehouse_id, district_id, 10, 
                                                    level1, true);
        return level0;
    }

    OrderStatusEager0*
    gen_order_status() {
        uint32_t warehouse_id = m_util.gen_rand_range(0, s_num_warehouses-1);
        uint32_t district_id = m_util.gen_rand_range(0, s_districts_per_wh-1);
        uint32_t customer_id = (uint32_t)m_util.gen_customer_id();
        
        OrderStatusEager1 *level1 = new OrderStatusEager1(warehouse_id, district_id, 
                                                          customer_id, NULL, false);
        OrderStatusEager0 *level0 = new OrderStatusEager0(warehouse_id, district_id,
                                                      customer_id, NULL, false, 
                                                          level1, true);        
        return level0;
    }
};


#endif // EAGER_TPCC_GENERATOR_HH_
