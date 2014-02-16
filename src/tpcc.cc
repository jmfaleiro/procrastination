// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// Adapted from oltpbench (git@github.com:oltpbenchmark/oltpbench.git)

#include <tpcc.hh>
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

using namespace std;

Warehouse 									*s_warehouse_tbl;
Item 										*s_item_tbl;
StringTable<Customer*> 						*s_last_name_index;
ConcurrentHashTable<uint64_t, History>		*s_history_tbl;
ConcurrentHashTable<uint64_t, NewOrder> 	*s_new_order_tbl;
ConcurrentHashTable<uint64_t, Oorder> 		*s_oorder_tbl;
ConcurrentHashTable<uint64_t, OrderLine> 	*s_order_line_tbl;
uint32_t 									s_num_items;  
uint32_t 									s_num_warehouses;
uint32_t 									s_districts_per_wh;
uint32_t 									s_customers_per_dist;

TPCCInit::TPCCInit(uint32_t num_warehouses, uint32_t dist_per_wh, 
                         uint32_t cust_per_dist, uint32_t item_count) {
    m_num_warehouses = num_warehouses;
    m_dist_per_wh = dist_per_wh;
    m_cust_per_dist = cust_per_dist;
    m_item_count = item_count;
}

static int
rand_range(int min, int max) {
    int range_len = max - min + 1;
    int ret = min + (rand() % range_len);
    assert(ret >= min && ret <= max);
    return ret;
}

void
TPCCInit::init_warehouse(Warehouse *warehouse, TPCCUtil &random) {
    for (uint32_t i = 0; i < m_num_warehouses; ++i) {
        warehouse[i].w_id = i;
        warehouse[i].w_ytd = 30000.0;
        warehouse[i].w_tax = (rand() % 2001) / 1000.0;
        
        // Generate a bunch of random strings for the string fields. 
        random.gen_rand_string(6, 10, warehouse[i].w_name);
        random.gen_rand_string(10, 20, warehouse[i].w_street_1);
        random.gen_rand_string(10, 20, warehouse[i].w_street_2);
        random.gen_rand_string(10, 20, warehouse[i].w_city);
        random.gen_rand_string(3, 3, warehouse[i].w_state);
        
        char stupid_zip[] = "123456789";
        strcpy(warehouse[i].w_zip, stupid_zip);

        warehouse[i].w_district_table = NULL;
        warehouse[i].w_stock_table = NULL;        
    }
}

// Initialize the district table of one particular warehouse. 
void
TPCCInit::init_district(District *district, uint32_t warehouse_id, 
                        TPCCUtil &random) {
    for (uint32_t i = 0; i < m_dist_per_wh; ++i) {
        district[i].d_id = i;
        district[i].d_w_id = warehouse_id;
        district[i].d_ytd = 3000;
        district[i].d_tax = (rand() % 2001) / 1000.0;
        district[i].d_next_o_id = 3001;

        random.gen_rand_string(6, 10, district[i].d_name);
        random.gen_rand_string(10, 20, district[i].d_street_1);
        random.gen_rand_string(10, 20, district[i].d_street_2);
        random.gen_rand_string(10, 20, district[i].d_city);
        random.gen_rand_string(3, 3, district[i].d_state);
        
        char contiguous_zip[] = "123456789";
        strcpy(district[i].d_zip, contiguous_zip);
        district[i].d_customer_table = NULL;
    }
}

void
TPCCInit::init_customer(Customer *customer, uint32_t d_id, 
                        uint32_t w_id, TPCCUtil &random) {
    for (uint32_t i = 0; i < m_cust_per_dist; ++i) {
        customer[i].c_id = i;
        customer[i].c_d_id = d_id;
        customer[i].c_w_id = w_id;
        
        // Discount in the range [0.0000 ... 0.5000]
        customer[i].c_discount = (rand() % 5001) / 10000.0;
        
        
        if (rand() % 101 <= 10) {		// 10% Bad Credit
            customer[i].c_credit[0] = 'B';
            customer[i].c_credit[1] = 'C';
            customer[i].c_credit[2] = '\0';
        }
        else {		// 90% Good Credit
            customer[i].c_credit[0] = 'G';
            customer[i].c_credit[1] = 'C';
            customer[i].c_credit[2] = '\0';
        }                
        random.gen_rand_string(8, 16, customer[i].c_first);
        random.gen_last_name_load(customer[i].c_last);
        s_last_name_index->Put(customer[i].c_last, &customer[i]);

        customer[i].c_credit_lim = 50000;
        customer[i].c_balance = -10;
        customer[i].c_ytd_payment = 10;
        customer[i].c_payment_cnt = 1;
        customer[i].c_delivery_cnt = 0;        

        random.gen_rand_string(10, 20, customer[i].c_street_1);
        random.gen_rand_string(10, 20, customer[i].c_street_2);
        random.gen_rand_string(10, 20, customer[i].c_city);
        random.gen_rand_string(3, 3, customer[i].c_state);
        random.gen_rand_string(4, 4, customer[i].c_zip);
        
        for (int j = 4; j < 9; ++j) {
            customer[i].c_zip[j] = '1';            
        }
        random.gen_rand_string(16, 16, customer[i].c_phone);
        
        customer[i].c_middle[0] = 'O';
        customer[i].c_middle[1] = 'E';
        customer[i].c_middle[2] = '\0';
        
        random.gen_rand_string(300, 500, customer[i].c_data);
    }
}

void
TPCCInit::init_history(History *history, TPCCUtil &random) {

}

void
TPCCInit::init_order(TPCCUtil &random) {
    Oorder oorder;
    NewOrder new_order;
    OrderLine order_line;

    assert(s_oorder_tbl != NULL);
    assert(s_new_order_tbl != NULL);
    assert(s_order_line_tbl != NULL);

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
                s_oorder_tbl->Put(oorder_key, oorder);

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
TPCCInit::init_item(Item *item, TPCCUtil &random) {
    for (uint32_t i = 0; i < m_item_count; ++i) {
        item[i].i_id = i;
        random.gen_rand_string(14, 24, item[i].i_name);
        item[i].i_price = (100 + (rand() % 9900)) / 100.0;
        int rand_pct = random.gen_rand_range(0, 99);
        int len = random.gen_rand_range(26, 50);
        
        random.gen_rand_string(len, len, item[i].i_data);
        if (rand_pct <= 10) {

            // 10% of the time i_data has "ORIGINAL" crammed somewhere in the
            // middle. 
            int original_start = rand_range(2, len-8);
            item[i].i_data[original_start] = 'O';
            item[i].i_data[original_start+1] = 'R';
            item[i].i_data[original_start+2] = 'I';
            item[i].i_data[original_start+3] = 'G';
            item[i].i_data[original_start+4] = 'I';
            item[i].i_data[original_start+5] = 'N';
            item[i].i_data[original_start+6] = 'A';
            item[i].i_data[original_start+7] = 'L';
        }
        item[i].i_im_id = 1 + (rand() % 10000);
    }
}

void
TPCCInit::init_stock(Stock *stock, uint32_t warehouse_id, 
                     TPCCUtil &random) {
    Stock container;
    int randPct;
    int len;
    int start_original;
    for (uint32_t i = 0; i < m_item_count; ++i) {
        container.s_i_id = i;
        container.s_w_id = warehouse_id;
        container.s_quantity = 10 + rand() % 90;
        container.s_ytd = 0;
        container.s_order_cnt = 0;
        container.s_remote_cnt = 0;

        // s_data
        randPct = rand() % 100;
        len = rand_range(26, 50);

        random.gen_rand_string(len, len, container.s_data);
        if (randPct <= 10) {
            
            // 10% of the time, i_data has the string "ORIGINAL" crammed 
            // somewhere in the middle.
            start_original = rand_range(2, len - 8);
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
    }
}

void
TPCCInit::do_init() {
    
    TPCCUtil random;
    s_num_items = m_item_count;
    s_num_warehouses = m_num_warehouses;
    s_districts_per_wh = m_dist_per_wh;
    s_customers_per_dist = m_cust_per_dist;

    s_new_order_tbl = new ConcurrentHashTable<uint64_t, NewOrder>(1<<19, 20);
    s_oorder_tbl = new ConcurrentHashTable<uint64_t, Oorder>(1<<19, 20);
    s_order_line_tbl = new ConcurrentHashTable<uint64_t, OrderLine>(1<<19, 20);
    s_last_name_index = new StringTable<Customer*>(1<<19, 20);
    s_history_tbl = new ConcurrentHashTable<uint64_t, History>(1<<19, 20);

    NewOrder blah;
    for (uint64_t i = 0; i < 10000000; ++i) {
        s_new_order_tbl->Put(0, blah);
    }

    cout << "Num warehouses: " << m_num_warehouses << "\n";
    cout << "Num districts: " << m_dist_per_wh << "\n";
    cout << "Num customers: " << m_cust_per_dist << "\n";
    cout << "Num items: " << m_item_count << "\n";

    init_order(random);
    s_item_tbl = (Item*)malloc(sizeof(Item)*m_item_count);
    memset(s_item_tbl, 0, sizeof(Item)*m_item_count);
    init_item(s_item_tbl, random);

    // First initialize all the warehouses in the system.
    s_warehouse_tbl = (Warehouse*)malloc(sizeof(Warehouse)*m_num_warehouses);
    init_warehouse(s_warehouse_tbl, random);
    for (uint32_t w = 0; w < m_num_warehouses; ++w) {
        s_warehouse_tbl[w].w_district_table = 
            (District*)malloc(sizeof(District)*m_dist_per_wh);
        init_district(s_warehouse_tbl[w].w_district_table, w, random);
        for (uint32_t d = 0; d < m_dist_per_wh; ++d) {
            Customer *customer = 
                (Customer*)malloc(sizeof(Customer)*m_cust_per_dist);
            s_warehouse_tbl[w].w_district_table[d].d_customer_table = customer; 
            init_customer(customer, d, w, random);
        }
        s_warehouse_tbl[w].w_stock_table = 
            (Stock*)malloc(sizeof(Stock)*m_item_count);
        init_stock(s_warehouse_tbl[w].w_stock_table, w, random);
    }    
}

// Append table identifier to the primary key. Use this string to identify 
// records in a transaction's read/write sets. 
/*
  static inline string
  CreateKey(string table_name, string key) {
  ostringstream os;
  os << table_name << "###" <<< key;
  return os.str();
  }

  // Parse the table name from the composite key. 
  static inline string
  GetTable(string key) {
  int pkey_index = key.find("###");
  return key.substr(0, pkey_index);
  }

  // Parse the primary key from the composite key. 
  static inline string
  GetPKey(string key) {
  int pkey_index = key.find("###");
  return key.substr(pkey_index);
  }
*/

/* Read the customer table <w_id, d_id, c_id> 
 * Read the warehouse table <w_id>
 * Update district table <w_id, d_id>
 * Insert into the new order table. XXX: Does this need co-ordination?
 * Insert into the open order table. XXX: Does this need co-ordination?
 * Update the stock table. 
 *
 * Read set: 	Customer key
 * 		   		Warehouse key 
 *
 * Write set: 	District key 
 *				Stock key (for each item)
 */
NewOrderTxn::NewOrderTxn(uint64_t w_id, uint64_t d_id, uint64_t c_id, 
                         uint64_t o_all_local, uint64_t numItems, 
                         uint64_t *itemIds, 
                         uint64_t *supplierWarehouseIDs, 
                         uint32_t *orderQuantities) {
    uint32_t keys[10];
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;

    uint64_t table_id;

    // Create the warehouse, customer, and district keys. 
    keys[0] = w_id;
    keys[1] = d_id;
    keys[2] = c_id;
    uint64_t warehouse_key = w_id;
    uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
    uint64_t district_key = TPCCKeyGen::create_district_key(keys);
  
    // Insert the customer key in the read set. 
    dep_info.record.m_table = CUSTOMER;
    dep_info.record.m_key = customer_key;
    readset.push_back(dep_info);
  
    // Insert the warehouse key in the read set. 
    dep_info.record.m_table = WAREHOUSE;
    dep_info.record.m_key = warehouse_key;
    readset.push_back(dep_info);
  
    // Insert the district key in the write set.
    dep_info.record.m_table = DISTRICT;
    dep_info.record.m_key = district_key;
    writeset.push_back(dep_info);

    // Create stock, and item keys for each item. 
    for (uint32_t i = 0; i < numItems; ++i) {

        // Create the item and stock keys. 
        uint64_t item_key = itemIds[i];
        assert(itemIds[i] == invalid_item_key || itemIds[i] < s_num_items);
        keys[0] = supplierWarehouseIDs[i];
        keys[1] = itemIds[i];
        uint64_t stock_key = TPCCKeyGen::create_stock_key(keys);

        // Insert the item key into the read set. 
        assert(item_key == invalid_item_key || item_key < s_num_items);
        dep_info.record.m_key = item_key;
        dep_info.record.m_table = ITEM;
        readset.push_back(dep_info);

        // Insert the stock key into the write set. 
        dep_info.record.m_key = stock_key;
        dep_info.record.m_table = STOCK;
        writeset.push_back(dep_info);
    }
    m_order_quantities = orderQuantities;
    m_supplierWarehouse_ids = supplierWarehouseIDs;
    m_num_items = numItems;
    m_all_local = (int)o_all_local;
}

bool
NewOrderTxn::NowPhase() {
    CompositeKey composite;

    // A NewOrder txn aborts if any of the item keys are invalid. 
    int num_items = readset.size();
    for (int i = s_item_index; i < num_items; ++i) {

        // Make sure that all the items requested exist. 
        composite = readset[i].record;
        assert(composite.m_table == ITEM);
        uint64_t item_key = composite.m_key;
        if (item_key == invalid_item_key) {
            return false;	// Abort the txn. 
        }
    }
  
    // We are guaranteed not to abort once we're here. Increment the highly 
    // contended next_order_id in the district table.
    composite = writeset[s_district_index].record;
    uint64_t district_key = TPCCKeyGen::get_district_key(composite.m_key);
    uint64_t warehouse_key = TPCCKeyGen::get_warehouse_key(composite.m_key);
    District *district_tbl = s_warehouse_tbl[warehouse_key].w_district_table;
    District *district = &district_tbl[district_key];
  
    // Update the district record. 
    m_order_id = district->d_next_o_id;
    m_district_tax = district->d_tax;
    district->d_next_o_id += 1;
    
    NewOrder blah;
    s_new_order_tbl->Put(0, blah);
    m_timestamp = time(NULL);
    return true;		// The txn can be considered committed. 
}

void
NewOrderTxn::LaterPhase() {
    float total_amount = 0;
    CompositeKey composite;
    uint32_t keys[5];
    ostringstream os; 
    
    // Read the customer record.
    composite = readset[s_customer_index].record;
    assert(composite.m_table == CUSTOMER);
    uint64_t w_id = TPCCKeyGen::get_warehouse_key(composite.m_key);
    uint64_t d_id = TPCCKeyGen::get_district_key(composite.m_key);
    uint64_t c_id = TPCCKeyGen::get_customer_key(composite.m_key);
  
    Customer *customer = 
        &(s_warehouse_tbl[w_id].w_district_table[d_id].d_customer_table[c_id]);

    float c_discount = customer->c_discount;
    //    string c_credit = customer->c_credit;

    // Read the warehouse record.
    Warehouse *warehouse = &s_warehouse_tbl[w_id];
    float w_tax = warehouse->w_tax;  

    // Create a NewOrder record. 
    // XXX: This is a potentially serializing call to malloc. Make sure to link
    // TCMalloc so that allocations can be (mostly) thread-local. 
    NewOrder new_order_record;

    //    new_order_record.no_w_id = w_id;
    //    new_order_record.no_d_id = d_id;
    //    new_order_record.no_o_id = m_order_id;
    keys[0] = w_id;
    keys[1] = d_id;
    keys[2] = m_order_id;

    // Generate the NewOrder key and insert the record.
    // XXX: This insertion has to be concurrent. Therefore, use a hash-table to 
    // implement it. 
    uint64_t no_id = TPCCKeyGen::create_new_order_key(keys);
    //    s_new_order_tbl->Put(m_order_id, new_order_record);
  
    for (int i = 0; i < m_num_items; ++i) {
        composite = writeset[s_stock_index+i].record;
        assert(composite.m_table == STOCK);
        uint64_t ol_s_id = TPCCKeyGen::get_stock_key(composite.m_key);
        uint64_t ol_w_id = TPCCKeyGen::get_warehouse_key(composite.m_key);
        uint64_t ol_i_id = readset[s_item_index+i].record.m_key;
        assert(readset[s_item_index+i].record.m_table == ITEM);
        
        if (ol_s_id != ol_i_id) {
            cout << "Stock item: " << ol_s_id;
            cout << "Item: " << ol_i_id << "\n";
        }
        assert(ol_i_id == ol_s_id);

        int ol_quantity = m_order_quantities[i];

        assert(ol_i_id < s_num_items);
    
        // Get the item and the stock records. 
        Item *item = &s_item_tbl[ol_i_id];
        Stock *stock = &s_warehouse_tbl[ol_w_id].w_stock_table[ol_s_id];
    
        // Update the inventory for the item in question. 
        if (stock->s_order_cnt - ol_quantity >= 10) {
            stock->s_quantity -= ol_quantity;
        }
        else {
            stock->s_quantity += -ol_quantity + 91;
        }    
        if (ol_w_id != w_id) {
            stock->s_remote_cnt += 1;
        }
        stock->s_ytd += ol_quantity;
        total_amount += ol_quantity * (item->i_price);

        char *ol_dist_info = NULL;
        switch (d_id) {
        case 0:
            ol_dist_info = stock->s_dist_01;
            break;
        case 1:
            ol_dist_info = stock->s_dist_02;
            break;
        case 2:
            ol_dist_info = stock->s_dist_03;
            break;
        case 3:
            ol_dist_info = stock->s_dist_04;
            break;
        case 4:
            ol_dist_info = stock->s_dist_05;
            break;
        case 5:
            ol_dist_info = stock->s_dist_06;
            break;
        case 6:
            ol_dist_info = stock->s_dist_07;
            break;
        case 7:
            ol_dist_info = stock->s_dist_08;
            break;
        case 8:
            ol_dist_info = stock->s_dist_09;
            break;
        case 9:
            ol_dist_info = stock->s_dist_10;
            break;
        default:
            std::cout << "Got unexpected district!!! Aborting...\n";
            std::cout << m_district_id << "\n";
            exit(0);
        }
        // Finally, insert an item into the order line table.
        // XXX: This memory allocation can serialize threads. Link TCMalloc to 
        // ensure that all allocations happen with minimal possible kernel-level 
        // synchronization.
        OrderLine new_order_line;
        new_order_line.ol_o_id = m_order_id;
        new_order_line.ol_d_id = d_id;
        new_order_line.ol_w_id = w_id;
        new_order_line.ol_number = i;
        new_order_line.ol_i_id = item->i_id;
        new_order_line.ol_supply_w_id = ol_w_id;
        new_order_line.ol_quantity = m_order_quantities[i];
        new_order_line.ol_amount = m_order_quantities[i] * (item->i_price);
        strcpy(new_order_line.ol_dist_info, ol_dist_info);

        keys[0] = w_id;
        keys[1] = d_id;
        keys[2] = m_order_id;
        keys[3] = i;
        uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);

        // XXX: Make sure that the put operation is implemented on top of a 
        // concurrent hash table. 
        s_order_line_tbl->Put(order_line_key, new_order_line);
    }
    
    // Insert an entry into the open order table.    
    /*
    Oorder oorder;
    oorder.o_id = m_order_id;
    oorder.o_d_id = d_id;
    oorder.o_w_id = w_id;
    oorder.o_c_id = c_id;
    oorder.o_entry_d = m_timestamp;
    oorder.o_ol_cnt = m_num_items;
    oorder.o_all_local = m_all_local;
    */
}

PaymentTxn::PaymentTxn(uint32_t w_id, uint32_t c_w_id, float h_amount, uint32_t d_id,
                       uint32_t c_d_id, uint32_t c_id, char *c_last, bool c_by_name) {
    assert(w_id < s_num_warehouses);
    assert(c_w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);

    uint32_t keys[10];
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;

    uint64_t table_id;

    // Create the warehouse, and district keys. Add them to the write set.
    keys[0] = w_id;
    keys[1] = d_id;
    uint64_t warehouse_key = w_id;
    uint64_t district_key = TPCCKeyGen::create_district_key(keys);

    keys[0] = c_w_id;
    keys[1] = c_d_id;
    keys[2] = c_id;
    uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
    
    assert((uint32_t)c_w_id == TPCCKeyGen::get_warehouse_key(customer_key));
    assert((uint32_t)c_d_id == TPCCKeyGen::get_district_key(customer_key));
    assert((uint32_t)c_id == TPCCKeyGen::get_customer_key(customer_key));

    dep_info.record.m_table = WAREHOUSE;
    dep_info.record.m_key = warehouse_key;
    writeset.push_back(dep_info);

    dep_info.record.m_table = DISTRICT;
    dep_info.record.m_key = district_key;
    writeset.push_back(dep_info);
      
    dep_info.record.m_table = CUSTOMER;
    dep_info.record.m_key = customer_key;
    writeset.push_back(dep_info);

    m_time = (uint32_t)time(NULL);
    m_h_amount = h_amount;
}    

bool
PaymentTxn::NowPhase() {
    uint64_t w_id = writeset[s_warehouse_index].record.m_key;
    uint64_t d_id = TPCCKeyGen::get_district_key(writeset[s_district_index].record.m_key);
    assert(writeset[s_warehouse_index].record.m_table == WAREHOUSE);
    assert(writeset[s_district_index].record.m_table == DISTRICT);
    assert(w_id < s_num_warehouses);    
    assert(d_id < s_districts_per_wh);

    // Update the warehouse and district
    s_warehouse_tbl[w_id].w_ytd += m_h_amount;
    District *dist = &s_warehouse_tbl[w_id].w_district_table[d_id];
    dist->d_ytd += m_h_amount;
    return true;
}

void
PaymentTxn::LaterPhase() {
    // Update the customer
    assert(writeset[s_customer_index].record.m_table == CUSTOMER);
    uint64_t w_id = 
        TPCCKeyGen::get_warehouse_key(writeset[s_customer_index].record.m_key);
    uint64_t d_id = 
        TPCCKeyGen::get_district_key(writeset[s_customer_index].record.m_key);
    uint64_t c_id = 
        TPCCKeyGen::get_customer_key(writeset[s_customer_index].record.m_key);
    
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);
    static const char *credit = "BC";
    Customer *cust = 
        &s_warehouse_tbl[w_id].w_district_table[d_id].d_customer_table[c_id];
    if (strcmp(credit, cust->c_credit) == 0) {	// Bad credit
        
        static const char *space = " ";
        char c_id_str[17];
        sprintf(c_id_str, "%lx", c_id);
        char c_d_id_str[17]; 
        sprintf(c_d_id_str, "%lx", d_id);
        char c_w_id_str[17];
        sprintf(c_w_id_str, "%lx", w_id);
        char d_id_str[17]; 
        sprintf(d_id_str, "%lx", writeset[s_district_index].record.m_key);
        char w_id_str[17];
        sprintf(w_id_str, "%lx", writeset[s_warehouse_index].record.m_key);
        char h_amount_str[17];
        sprintf(h_amount_str, "%lx", (uint64_t)m_h_amount);
        
        static const char *holder[11] = {c_id_str, space, c_d_id_str, space, 
                                         c_w_id_str, space, d_id_str, space, 
                                         w_id_str, space, h_amount_str};
        TPCCUtil::append_strings(cust->c_data, holder, 501, 11);
    }
    else {
        cust->c_balance -= m_h_amount;
        cust->c_ytd_payment += m_h_amount;
        cust->c_payment_cnt += 1;
    }

    // Insert an item into the History table
    History hist;
    hist.h_c_id = c_id;
    hist.h_c_d_id = d_id;
    hist.h_c_w_id = w_id;
    hist.h_d_id = 
        TPCCKeyGen::get_district_key(writeset[s_district_index].record.m_key);
    hist.h_w_id = writeset[s_warehouse_index].record.m_key;
    hist.h_date = m_time;
    hist.h_amount = m_h_amount;
    
    assert(writeset[s_warehouse_index].record.m_table == WAREHOUSE);
    assert(writeset[s_district_index].record.m_table == DISTRICT);
    uint64_t warehouse_index = writeset[s_warehouse_index].record.m_key;
    uint64_t district_index = writeset[s_district_index].record.m_key;
    Warehouse *wh = &s_warehouse_tbl[warehouse_index];
    District *dist = 
        &s_warehouse_tbl[warehouse_index].w_district_table[s_district_index];
    
    static const char *empty = "    ";
    const char *holder[3] = {wh->w_name, empty, dist->d_name};
    TPCCUtil::append_strings(hist.h_data, holder, 26, 3);
    s_history_tbl->Put(writeset[s_customer_index].record.m_key, hist);
}


/* Read the district table 			<w_id, d_id> 
 * Read the Stock table 			<s_w_id, s_i_id>
 * Read the Orderline table 		<ol_w_id, ol_d_id, ol_o_id, ol_number>
 *
vv * Read set: 	District key
 *				Orderline key + Stock key (20 records)
 */
/*
  StockLevelTxn::StockLevelTxn(int w_id, int d_id, int threshold) {
  ostringsrream os;
  os << w_id << "###" << d_id;
  string district_key = os.str();
  os.str(std::string());
  
  
  }

  bool
  StockLevelTxn::NowPhase() {
  return false;
  }

  void
  StockLevelTxn::LaterPhase() {
  
  }
*/

