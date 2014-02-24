// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// Adapted from oltpbench (git@github.com:oltpbenchmark/oltpbench.git)

#include <tpcc.hh>
#include <lock_manager.hh>
#include <cassert>
#include <string>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <random>
#include <time.h>
#include <string.h>
#include <iomanip>
#include <stdlib.h>
#include <algorithm>

using namespace std;

// Base tables indexed by primary key
HashTable<uint64_t, Warehouse> 						*s_warehouse_tbl;
HashTable<uint64_t, District> 						*s_district_tbl;
HashTable<uint64_t, Customer> 						*s_customer_tbl;
HashTable<uint64_t, Item> 							*s_item_tbl;
HashTable<uint64_t, Stock> 							*s_stock_tbl;

ConcurrentHashTable<uint64_t, Oorder>			 				*s_oorder_tbl;
ConcurrentHashTable<uint64_t, History> 						*s_history_tbl;
ConcurrentHashTable<uint64_t, NewOrder> 						*s_new_order_tbl;
ConcurrentHashTable<uint64_t, OrderLine> 						*s_order_line_tbl;

// Secondary indices
StringTable<Customer*>								*s_last_name_index;
HashTable<uint64_t, Oorder*>						*s_oorder_index;
HashTable<uint64_t, uint32_t>						*s_next_delivery_tbl;

LockManager *s_lock_manager;

// Experiment parameters
uint32_t 											s_num_tables;
uint32_t 											s_num_items;
uint32_t 											s_num_warehouses;
uint32_t 											s_districts_per_wh;
uint32_t 											s_customers_per_dist;

TPCCInit::TPCCInit(uint32_t num_warehouses, uint32_t dist_per_wh, 
                   uint32_t cust_per_dist, uint32_t item_count) {
    m_num_warehouses = num_warehouses;
    m_dist_per_wh = dist_per_wh;
    m_cust_per_dist = cust_per_dist;
    m_item_count = item_count;
}

void
TPCCInit::init_warehouses(TPCCUtil &random) {
    Warehouse temp;
    for (uint32_t i = 0; i < m_num_warehouses; ++i) {
        temp.w_id = i;
        temp.w_ytd = 30000.0;
        temp.w_tax = (rand() % 2001) / 1000.0;

        // Generate a bunch of random strings for the string fields. 
        random.gen_rand_string(6, 10, temp.w_name);
        random.gen_rand_string(10, 20, temp.w_street_1);
        random.gen_rand_string(10, 20, temp.w_street_2);
        random.gen_rand_string(10, 20, temp.w_city);
        random.gen_rand_string(3, 3, temp.w_state);        
        char stupid_zip[] = "123456789";
        strcpy(temp.w_zip, stupid_zip);

        uint64_t w_id = i;
        Warehouse *verif = s_warehouse_tbl->Put(w_id, temp);
        assert(s_warehouse_tbl->GetPtr(w_id) == verif);
    }
}

// Initialize the district table of one particular warehouse. 
void
TPCCInit::init_districts(TPCCUtil &random) {
    uint32_t keys[2];
    District district;
    for (uint32_t w_id = 0; w_id < s_num_warehouses; ++w_id) {
        keys[0] = w_id;
        for (uint32_t i = 0; i < m_dist_per_wh; ++i) {
            district.d_id = i;
            district.d_w_id = w_id;
            district.d_ytd = 3000;
            district.d_tax = (rand() % 2001) / 1000.0;
            district.d_next_o_id = 3000;

            random.gen_rand_string(6, 10, district.d_name);
            random.gen_rand_string(10, 20, district.d_street_1);
            random.gen_rand_string(10, 20, district.d_street_2);
            random.gen_rand_string(10, 20, district.d_city);
            random.gen_rand_string(3, 3, district.d_state);

            char contiguous_zip[] = "123456789";
            strcpy(district.d_zip, contiguous_zip);

            keys[1] = i;
            uint64_t district_key = TPCCKeyGen::create_district_key(keys);
            District *verif = s_district_tbl->Put(district_key, district);
            assert(s_district_tbl->GetPtr(district_key) == verif);            
            s_next_delivery_tbl->Put(district_key, s_first_unprocessed_o_id);
        }
    }
}

void
TPCCInit::init_customers(TPCCUtil &random) {
    uint32_t keys[3];
    Customer customer;

    for (uint32_t w_id = 0; w_id < s_num_warehouses; ++w_id) {
        keys[0] = w_id;
        for (uint32_t d_id = 0; d_id < s_districts_per_wh; ++d_id) {
            keys[1] = d_id;
            for (uint32_t i = 0; i < m_cust_per_dist; ++i) {
                customer.c_id = i;
                customer.c_d_id = d_id;
                customer.c_w_id = w_id;

                // Discount in the range [0.0000 ... 0.5000]
                customer.c_discount = (rand() % 5001) / 10000.0;        

                if (rand() % 101 <= 10) {		// 10% Bad Credit
                    customer.c_credit[0] = 'B';
                    customer.c_credit[1] = 'C';
                    customer.c_credit[2] = '\0';
                }
                else {		// 90% Good Credit
                    customer.c_credit[0] = 'G';
                    customer.c_credit[1] = 'C';
                    customer.c_credit[2] = '\0';
                }                
                random.gen_rand_string(8, 16, customer.c_first);
                random.gen_last_name_load(customer.c_last);
                s_last_name_index->Put(customer.c_last, &customer);

                customer.c_credit_lim = 50000;
                customer.c_balance = -10;
                customer.c_ytd_payment = 10;
                customer.c_payment_cnt = 1;
                customer.c_delivery_cnt = 0;        

                random.gen_rand_string(10, 20, customer.c_street_1);
                random.gen_rand_string(10, 20, customer.c_street_2);
                random.gen_rand_string(10, 20, customer.c_city);
                random.gen_rand_string(3, 3, customer.c_state);
                random.gen_rand_string(4, 4, customer.c_zip);

                for (int j = 4; j < 9; ++j) {
                    customer.c_zip[j] = '1';            
                }
                random.gen_rand_string(16, 16, customer.c_phone);

                customer.c_middle[0] = 'O';
                customer.c_middle[1] = 'E';
                customer.c_middle[2] = '\0';

                random.gen_rand_string(300, 500, customer.c_data);
                keys[2] = i;
                uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
                Customer *verif = s_customer_tbl->Put(customer_key, customer);
                assert(s_customer_tbl->GetPtr(customer_key) == verif);
            }
        }
    }
}

void
TPCCInit::init_history(TPCCUtil &random) {

}

void
TPCCInit::init_orders(TPCCUtil &random) {
    Oorder oorder;
    NewOrder new_order;
    OrderLine order_line;

    assert(s_oorder_tbl != NULL);
    assert(s_new_order_tbl != NULL);
    assert(s_order_line_tbl != NULL);
    assert(s_first_unprocessed_o_id < m_cust_per_dist);

    // Use this array to construct a composite key for each of the Order, 
    // OrderLine and Oorder tables. 
    uint32_t keys[5];

    for (uint32_t w = 0; w < m_num_warehouses; ++w) {
        for (uint32_t d = 0; d < m_dist_per_wh; ++d) {

            // Initialize the customer id array. 
            std::vector<uint32_t> c_ids;
            for (uint32_t k = 0; k < m_cust_per_dist; ++k) {
                c_ids.push_back(k);
            }
            std::shuffle(c_ids.begin(), c_ids.end(), 
                         std::default_random_engine((unsigned)time(NULL)));

            for (uint32_t c = 0; c < m_cust_per_dist; ++c) {
                oorder.o_id = c;
                oorder.o_w_id = w;
                oorder.o_d_id = d;
                oorder.o_c_id = c_ids[c];
                // o_carrier_id is set *only* for orders with ids < 2101 
                // [4.3.3.1]
                if (oorder.o_id < s_first_unprocessed_o_id) {
                    oorder.o_carrier_id = 1 + rand() % 10;
                }
                else {
                    oorder.o_carrier_id = 0;
                }
                oorder.o_ol_cnt = 5 + rand() % 11;
                oorder.o_all_local = 1;

                // Record the current time in terms of milliseconds elapsed 
                // since an event (not sure what the event is.. !).
                timespec temp_time; 
                clock_gettime(CLOCK_THREAD_CPUTIME_ID, &temp_time);
                oorder.o_entry_d = 
                    temp_time.tv_sec*1000.0 + temp_time.tv_nsec/1000000.0;

                // Create the oorder key and insert the record into the table.
                keys[0] = oorder.o_w_id;
                keys[1] = oorder.o_d_id;
                keys[2] = oorder.o_id;
                uint64_t oorder_key = TPCCKeyGen::create_order_key(keys);
                Oorder *inserted = s_oorder_tbl->Put(oorder_key, oorder);
                Oorder *inserted_verify = s_oorder_tbl->GetPtr(oorder_key);
                assert(inserted == inserted_verify && inserted != NULL);
                keys[2] = oorder.o_c_id;
                uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);                
                s_oorder_index->Put(customer_key, inserted);

                if (c >= s_first_unprocessed_o_id) {
                    new_order.no_w_id = w;
                    new_order.no_d_id = d;
                    new_order.no_o_id = c;

                    // Insert the record into the new order table.
                    keys[0] = new_order.no_w_id;
                    keys[1] = new_order.no_d_id;
                    keys[2] = new_order.no_o_id;
                    uint64_t no_key = TPCCKeyGen::create_new_order_key(keys);
                    s_new_order_tbl->Put(no_key, new_order);
                }

                for (uint32_t l = 0; l < oorder.o_ol_cnt; ++l) {
                    order_line.ol_w_id = w;
                    order_line.ol_d_id = d;
                    order_line.ol_o_id = c;
                    order_line.ol_number = l;
                    order_line.ol_i_id = 1 + rand() % 100000;

                    if (order_line.ol_o_id < s_first_unprocessed_o_id) {
                        order_line.ol_delivery_d = oorder.o_entry_d;
                        order_line.ol_amount = 0;
                    }
                    else {
                        order_line.ol_delivery_d = 0;
                        order_line.ol_amount = (1 + rand() % 999999) / 100.0;
                    }
                    order_line.ol_supply_w_id = order_line.ol_w_id;
                    order_line.ol_quantity = 5;
                    random.gen_rand_string(24, 24, order_line.ol_dist_info);

                    // Generate a key for the order line record and insert the 
                    // record into the table.
                    keys[0] = order_line.ol_w_id;
                    keys[1] = order_line.ol_d_id;
                    keys[2] = order_line.ol_o_id;
                    keys[3] = order_line.ol_number;
                    uint64_t order_line_key = 
                        TPCCKeyGen::create_order_line_key(keys);
                    s_order_line_tbl->Put(order_line_key, order_line);
                }
            }            
        }
    }
}

void
TPCCInit::init_items(TPCCUtil &random) {
    Item item;
    for (uint32_t i = 0; i < m_item_count; ++i) {
        item.i_id = i;
        random.gen_rand_string(14, 24, item.i_name);
        item.i_price = (100 + (rand() % 9900)) / 100.0;
        int rand_pct = random.gen_rand_range(0, 99);
        int len = random.gen_rand_range(26, 50);

        random.gen_rand_string(len, len, item.i_data);
        if (rand_pct <= 10) {

            // 10% of the time i_data has "ORIGINAL" crammed somewhere in the
            // middle. 
            int original_start = random.gen_rand_range(2, len-8);
            item.i_data[original_start] = 'O';
            item.i_data[original_start+1] = 'R';
            item.i_data[original_start+2] = 'I';
            item.i_data[original_start+3] = 'G';
            item.i_data[original_start+4] = 'I';
            item.i_data[original_start+5] = 'N';
            item.i_data[original_start+6] = 'A';
            item.i_data[original_start+7] = 'L';
        }
        item.i_im_id = 1 + (rand() % 10000);
        uint64_t item_key = (uint64_t)i;
        Item *verify = s_item_tbl->Put(item_key, item);
        assert(s_item_tbl->GetPtr(item_key) == verify);
    }
}

void
TPCCInit::init_stock(TPCCUtil &random) {
    Stock container;
    int randPct;
    int len;
    int start_original;

    uint32_t keys[2];
    for (uint32_t w_id = 0; w_id < s_num_warehouses; ++w_id) {
        keys[0] = w_id;
        for (uint32_t i = 0; i < m_item_count; ++i) {
            container.s_i_id = i;
            container.s_w_id = w_id;
            container.s_quantity = 10 + rand() % 90;
            container.s_ytd = 0;
            container.s_order_cnt = 0;
            container.s_remote_cnt = 0;

            // s_data
            randPct = random.gen_rand_range(1, 100);
            len = random.gen_rand_range(26, 50);

            random.gen_rand_string(len, len, container.s_data);
            if (randPct <= 10) {

                // 10% of the time, i_data has the string "ORIGINAL" crammed 
                // somewhere in the middle.
                start_original = random.gen_rand_range(2, len-8);
                container.s_data[start_original] = 'O';
                container.s_data[start_original+1] = 'R';
                container.s_data[start_original+2] = 'I';
                container.s_data[start_original+3] = 'G';
                container.s_data[start_original+4] = 'I';
                container.s_data[start_original+5] = 'N';
                container.s_data[start_original+6] = 'A';
                container.s_data[start_original+7] = 'L';            
            }

            random.gen_rand_string(24, 24, container.s_dist_01);
            random.gen_rand_string(24, 24, container.s_dist_02);
            random.gen_rand_string(24, 24, container.s_dist_03);
            random.gen_rand_string(24, 24, container.s_dist_04);
            random.gen_rand_string(24, 24, container.s_dist_05);
            random.gen_rand_string(24, 24, container.s_dist_06);
            random.gen_rand_string(24, 24, container.s_dist_07);
            random.gen_rand_string(24, 24, container.s_dist_08);
            random.gen_rand_string(24, 24, container.s_dist_09);
            random.gen_rand_string(24, 24, container.s_dist_10);

            keys[1] = i;
            uint64_t stock_key = TPCCKeyGen::create_stock_key(keys);
            Stock *verify = s_stock_tbl->Put(stock_key, container);
            assert(s_stock_tbl->GetPtr(stock_key) == verify);
        }
    }
}

void
TPCCInit::test_init() {
    uint32_t keys[4];
    for (uint32_t i = 0; i < s_num_warehouses; ++i) {
        keys[0] = i;	// Warehouse number
        for (uint32_t j = 0; j < s_districts_per_wh; ++j) {            
            keys[1] = j;	// District number
            uint64_t district_key = TPCCKeyGen::create_district_key(keys);
            District *dist = s_district_tbl->GetPtr(district_key);
            assert(dist != NULL);

            uint32_t next_order_id = dist->d_next_o_id;

            // Make sure that a stock level txn won't fail
            for (uint32_t k = 0; k < 20; ++k) {
                keys[2] = next_order_id - 1 - k;
                uint64_t open_order_key = TPCCKeyGen::create_order_key(keys);
                assert(s_oorder_tbl->GetPtr(open_order_key) != NULL);
            }

            // Make sure that the delivery txn won't fail
            uint32_t next_delivery_order = s_next_delivery_tbl->Get(district_key);
            keys[2] = next_delivery_order;
            uint64_t next_d_order_key = TPCCKeyGen::create_order_key(keys);
            assert(next_delivery_order == s_first_unprocessed_o_id);
            assert(s_oorder_tbl->GetPtr(next_d_order_key) != NULL);

            // Make sure that each customer has a sensible open order
            for (uint32_t k = 0; k < s_customers_per_dist; ++k) {
                keys[2] = k;
                uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
                assert(s_oorder_index->GetPtr(customer_key) != NULL);
            }            
        }
    }
}

void
TPCCInit::do_init() {    
    TPCCUtil random;
    s_num_tables = 10;
    s_num_items = m_item_count;
    s_num_warehouses = m_num_warehouses;
    s_districts_per_wh = m_dist_per_wh;
    s_customers_per_dist = m_cust_per_dist;

    s_warehouse_tbl = new HashTable<uint64_t, Warehouse>(1<<7, 20);
    s_district_tbl = new HashTable<uint64_t, District>(1<<10, 20);
    s_customer_tbl = new HashTable<uint64_t, Customer>(1<<24, 20);
    s_stock_tbl = new HashTable<uint64_t, Stock>(1<<24, 20);
    s_item_tbl = new HashTable<uint64_t, Item>(1<<24, 20);
    s_next_delivery_tbl = new HashTable<uint64_t, uint32_t>(1<<10, 20);

    s_new_order_tbl = new ConcurrentHashTable<uint64_t, NewOrder>(1<<20, 20);
    s_oorder_tbl = new ConcurrentHashTable<uint64_t, Oorder>(1<<20, 20);
    s_order_line_tbl = new ConcurrentHashTable<uint64_t, OrderLine>(1<<20, 20);
    s_last_name_index = new StringTable<Customer*>(1<<24, 20);
    s_history_tbl = new ConcurrentHashTable<uint64_t, History>(1<<20, 20);
    s_oorder_index = new HashTable<uint64_t, Oorder*>(1<<24, 20);

    cout << "Num warehouses: " << m_num_warehouses << "\n";
    cout << "Num districts: " << m_dist_per_wh << "\n";
    cout << "Num customers: " << m_cust_per_dist << "\n";
    cout << "Num items: " << m_item_count << "\n";

    init_warehouses(random);
    init_districts(random);
    init_customers(random);
    init_items(random);
    init_stock(random);
    init_orders(random);

    test_init();
}

Customer*
customer_by_name(char *name, uint32_t c_w_id, uint32_t c_d_id) {
    assert(c_w_id < s_num_warehouses);
    assert(c_d_id < s_districts_per_wh);

    static Customer *seen[100];
    int num_seen = 0;
    TableIterator<char*, Customer*> iter = s_last_name_index->GetIterator(name);
    while (!iter.Done()) {
        Customer *cust = iter.Value();
        if (cust->c_w_id == c_w_id && cust->c_d_id == c_d_id) {
            seen[num_seen++] = cust;
        }
        iter.Next();
    }

    if (num_seen != 0) {
        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but 
        // that counts starting from 1.    
        int index = num_seen / 2;
        if (num_seen % 2 == 0) {
            index -= 1;
        }
        return seen[index];
    }
    else {
        return NULL;
    }
}


