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

     // Create stock, and item keys for each item. 
     for (uint32_t i = 0; i < numItems; ++i) {

         // Create the item and stock keys. 
         uint64_t item_key = itemIds[i];
         assert(itemIds[i] == invalid_item_key || itemIds[i] < s_num_items);
         keys[0] = supplierWarehouseIDs[i];
         keys[1] = itemIds[i];
         uint64_t stock_key = TPCCKeyGen::create_stock_key(keys);
         uint32_t blah = TPCCKeyGen::get_stock_key(stock_key);
         // Insert the item key into the read set. 
         /*
         assert(item_key == invalid_item_key || item_key < s_num_items);
         dep_info.record.m_key = item_key;
         dep_info.record.m_table = ITEM;
         readset.push_back(dep_info);
         */

         // Insert the stock key into the write set.        
         dep_info.record.m_key = stock_key;
         dep_info.record.m_table = STOCK;
         writeset.push_back(dep_info);        
     }    

     dep_info.record.m_table = NEW_ORDER;
     dep_info.record.m_key = 0;
     writeset.push_back(dep_info);
     
     /*
     for (uint32_t i = 0; i < numItems; ++i) {
         dep_info.record.m_table = ORDER_LINE;
         dep_info.record.m_key = 0;
         writeset.push_back(dep_info);
     }
     */

     dep_info.record.m_table = OPEN_ORDER;
     dep_info.record.m_key = 0;
     writeset.push_back(dep_info);

     dep_info.record.m_table = OPEN_ORDER_INDEX;
     dep_info.record.m_key = customer_key;
     writeset.push_back(dep_info);

     m_order_quantities = orderQuantities;
     m_supplierWarehouse_ids = supplierWarehouseIDs;
     m_num_items = numItems;
     m_customer_id = c_id;
     m_warehouse_id = w_id;
     m_district_id = d_id;
     m_all_local = (int)o_all_local;
     m_item_ids = itemIds;
 }

 bool
 NewOrderTxn::NowPhase() {
     CompositeKey composite;
     uint32_t keys[4];

     // A NewOrder txn aborts if any of the item keys are invalid. 
     for (int i = 0; i < m_num_items; ++i) {

         // Make sure that all the items requested exist. 
         if (m_item_ids[i] == invalid_item_key) {
             return false;
         }
     }

     // We are guaranteed not to abort once we're here. Increment the highly 
     // contended next_order_id in the district table.
     keys[0] = m_warehouse_id;
     keys[1] = m_district_id;
     uint64_t district_key = TPCCKeyGen::create_district_key(keys);
     District *district = s_district_tbl->GetPtr(district_key);

     // Update the district record. 
     m_order_id = district->d_next_o_id;
     m_district_tax = district->d_tax;
     district->d_next_o_id += 1;

     m_warehouse_tax = s_warehouse_tbl->GetPtr(m_warehouse_id)->w_tax;
     m_timestamp = time(NULL);

     keys[2] = m_order_id;
     uint64_t new_order_key = TPCCKeyGen::create_new_order_key(keys);
     writeset[1].record.m_key = new_order_key;
     writeset[2].record.m_key = new_order_key;
     //     writeset[m_num_items].record.m_key = new_order_key;
     /*
     for (int i = 0; i < m_num_items; ++i) {
         keys[3] = (uint32_t)i;
         writeset[m_num_items+1+i].record.m_key = 
             TPCCKeyGen::create_order_line_key(keys);
     } 

     writeset[m_num_items+1].record.m_key = new_order_key;
     writeset[2*m_num_items+1].record.m_key = new_order_key;    
     */
    return true;		// The txn can be considered committed. 
}

void
NewOrderTxn::LaterPhase() {
    float total_amount = 0;
    CompositeKey composite;
    
    // Read the customer record.
    composite = readset[s_customer_index].record;
    assert(composite.m_table == CUSTOMER);
    Customer *customer = s_customer_tbl->GetPtr(composite.m_key);

    float c_discount = customer->c_discount;

    // Create a NewOrder record. 
    // XXX: This is a potentially serializing call to malloc. Make sure to link
    // TCMalloc so that allocations can be (mostly) thread-local. 
    NewOrder new_order_record;
    new_order_record.no_w_id = m_warehouse_id;
    new_order_record.no_d_id = m_district_id;
    new_order_record.no_o_id = m_order_id;

    // Generate the NewOrder key and insert the record.
    // XXX: This insertion has to be concurrent. Therefore, use a hash-table to 
    // implement it. 
    assert(writeset[m_num_items].record.m_table == NEW_ORDER);
    s_new_order_tbl->Put(writeset[m_num_items].record.m_key, new_order_record);
  
    for (int i = 0; i < m_num_items; ++i) {
        composite = writeset[s_stock_index+i].record;
        assert(composite.m_table == STOCK);
        uint64_t ol_w_id = TPCCKeyGen::get_warehouse_key(composite.m_key);
        uint64_t ol_s_id = TPCCKeyGen::get_stock_key(composite.m_key);
        uint64_t ol_i_id = readset[s_item_index+i].record.m_key;

        assert(readset[s_item_index+i].record.m_table == ITEM);        
        assert(ol_i_id == ol_s_id);
        assert(ol_i_id < s_num_items);

        int ol_quantity = m_order_quantities[i];
    
        // Get the item and the stock records. 
        Item *item = s_item_tbl->GetPtr(ol_i_id);
        Stock *stock = s_stock_tbl->GetPtr(composite.m_key);
    
        // Update the inventory for the item in question. 
        if (stock->s_order_cnt - ol_quantity >= 10) {
            stock->s_quantity -= ol_quantity;
        }
        else {
            stock->s_quantity += -ol_quantity + 91;
        }    
        if (ol_w_id != m_warehouse_id) {
            stock->s_remote_cnt += 1;
        }
        stock->s_ytd += ol_quantity;
        total_amount += ol_quantity * (item->i_price);

        char *ol_dist_info = NULL;
        switch (m_district_id) {
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
        new_order_line.ol_d_id = m_district_id;
        new_order_line.ol_w_id = m_warehouse_id;
        new_order_line.ol_number = i;
        new_order_line.ol_i_id = item->i_id;
        new_order_line.ol_supply_w_id = ol_w_id;
        new_order_line.ol_quantity = m_order_quantities[i];
        new_order_line.ol_amount = m_order_quantities[i] * (item->i_price);
        strcpy(new_order_line.ol_dist_info, ol_dist_info);

        // XXX: Make sure that the put operation is implemented on top of a 
        // concurrent hash table. 
        s_order_line_tbl->Put(writeset[m_num_items+1+i].record.m_key, new_order_line);
    }
    // Insert an entry into the open order table.    
    Oorder oorder;
    oorder.o_id = m_order_id;
    oorder.o_w_id = m_warehouse_id;
    oorder.o_d_id = m_district_id;
    oorder.o_c_id = m_customer_id;
    oorder.o_carrier_id = 0;
    oorder.o_ol_cnt = m_num_items;
    oorder.o_all_local = m_all_local;
    oorder.o_entry_d = m_timestamp;
    Oorder *new_oorder = s_oorder_tbl->Put(writeset[2*m_num_items+1].record.m_key, oorder);
    Oorder **old_index = s_oorder_index->GetPtr(readset[s_customer_index].record.m_key);
    *old_index = new_oorder;    
}

PaymentTxn::PaymentTxn(uint32_t w_id, uint32_t c_w_id, float h_amount, 
                       uint32_t d_id, uint32_t c_d_id, uint32_t c_id, 
                       char *c_last, bool c_by_name) {
    assert(w_id < s_num_warehouses);
    assert(c_w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);

    m_w_id = w_id;
    m_d_id = d_id;
    m_c_id = c_id;
    m_c_w_id = c_w_id;
    m_c_d_id = c_d_id;

    m_last_name = c_last;
    m_by_name = c_by_name;
    
    // Add the customer to the writeset.
    uint32_t keys[3];
    keys[0] = m_c_w_id;
    keys[1] = m_c_d_id;
    keys[2] = m_c_id;
    uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);

    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = CUSTOMER;
    dep_info.record.m_key = customer_key;
    writeset.push_back(dep_info);

    m_time = (uint32_t)time(NULL);
    m_h_amount = h_amount;
}    

bool
PaymentTxn::NowPhase() {
    /*
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    
    bool commit = true;
    
    if (m_by_name) {
        Customer *customer = customer_by_name(m_last_name, m_c_w_id, m_c_d_id);
        if (customer != NULL) {		// We found a valid customer            
            m_c_id = customer->c_id;

            // Update the warehouse
            Warehouse *warehouse = s_warehouse_tbl->GetPtr(m_w_id);
            warehouse->w_ytd += m_h_amount;

            // Update the district
            District *district = s_district_tbl->GetPtr(m_d_id);
            district->d_ytd += m_h_amount;
    
            m_warehouse_name = warehouse->w_name;
            m_district_name = district->d_name;
        }
        else {
            return false;
        }
    }
    */

    // Update the warehouse
    Warehouse *warehouse = s_warehouse_tbl->GetPtr(m_w_id);
    warehouse->w_ytd += m_h_amount;

    // Update the district
    District *district = s_district_tbl->GetPtr(m_d_id);
    district->d_ytd += m_h_amount;
    
    m_warehouse_name = warehouse->w_name;
    m_district_name = district->d_name;
    return true;
}

void
PaymentTxn::LaterPhase() {
    assert(writeset[s_customer_index].record.m_table == CUSTOMER);
    Customer *cust = 
        s_customer_tbl->GetPtr(writeset[s_customer_index].record.m_key);
    uint32_t customer_id = cust->c_id;

    static const char *credit = "BC";
    if (strcmp(credit, cust->c_credit) == 0) {	// Bad credit
        
        static const char *space = " ";
        char c_id_str[17];
        sprintf(c_id_str, "%x", m_c_id);
        char c_d_id_str[17]; 
        sprintf(c_d_id_str, "%x", m_c_d_id);
        char c_w_id_str[17];
        sprintf(c_w_id_str, "%x", m_c_w_id);
        char d_id_str[17]; 
        sprintf(d_id_str, "%x", m_w_id);
        char w_id_str[17];
        sprintf(w_id_str, "%x", m_d_id);
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
    hist.h_c_id = m_c_id;
    hist.h_c_d_id = m_c_d_id;
    hist.h_c_w_id = m_c_w_id;
    hist.h_d_id = m_d_id;
    hist.h_w_id = m_w_id;
    hist.h_date = m_time;
    hist.h_amount = m_h_amount;
    
    static const char *empty = "    ";
    const char *holder[3] = {m_warehouse_name, empty, m_district_name};
    TPCCUtil::append_strings(hist.h_data, holder, 26, 3);
    s_history_tbl->Put(writeset[s_customer_index].record.m_key, hist);
}

StockLevelTxn0::StockLevelTxn0(uint32_t warehouse_id, uint32_t district_id, 
                               int threshold, StockLevelTxn1 *level1_txn) {
    assert(warehouse_id < s_num_warehouses);
    assert(district_id < s_districts_per_wh);

    m_threshold = threshold;
    m_stock_count = 0;
    m_warehouse_id = warehouse_id;
    m_district_id = district_id;
    m_level1_txn = level1_txn;
}

bool
StockLevelTxn0::NowPhase() {
    uint32_t keys[2];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;
    uint64_t district_key = TPCCKeyGen::create_district_key(keys);
    m_next_order_id = s_district_tbl->GetPtr(district_key)->d_next_o_id;
    assert(m_next_order_id > 20);
    return true;
}

void
StockLevelTxn0::LaterPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = OPEN_ORDER;

    uint32_t keys[3];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;

    for (uint32_t i = 0; i < 20; ++i) {
        keys[2] = m_next_order_id - 1 -i;
        uint64_t open_order_key = TPCCKeyGen::create_order_key(keys);
        dep_info.record.m_key = open_order_key;
        m_level1_txn->readset.push_back(dep_info);
    }
    m_level1_txn->NowPhase();
    m_level1_txn->LaterPhase();
}

StockLevelTxn1::StockLevelTxn1(uint32_t warehouse_id, uint32_t district_id, 
                               int threshold, StockLevelTxn2 *level2_txn) : 
    StockLevelTxn0(warehouse_id, district_id, threshold, this) {       
    m_level2_txn = level2_txn;
}

bool
StockLevelTxn1::NowPhase() {
    return true;
}

void
StockLevelTxn1::LaterPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = ORDER_LINE;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;

    uint32_t num_reads = readset.size();
    for (uint32_t i = 0; i < num_reads; ++i) {
        Oorder *oorder = s_oorder_tbl->GetPtr(readset[i].record.m_key);
        keys[2] = oorder->o_id;
        for (uint32_t j = 0; j < oorder->o_ol_cnt; ++j) {
            keys[3] = j;
            uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
            dep_info.record.m_key = order_line_key;
            m_level2_txn->readset.push_back(dep_info);
        }
    }
    m_level2_txn->NowPhase();
    m_level2_txn->LaterPhase();
}

StockLevelTxn2::StockLevelTxn2(uint32_t warehouse_id, uint32_t district_id, 
                               int threshold, StockLevelTxn3 *level3_txn) : 
    StockLevelTxn1(warehouse_id, district_id, threshold, this) {        
    m_level3_txn = level3_txn;
}


bool
StockLevelTxn2::NowPhase() {
    return true;
}

void
StockLevelTxn2::LaterPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = STOCK;

    uint32_t keys[2];
    keys[0] = m_warehouse_id;
    
    uint32_t num_ol = readset.size();
    for (uint32_t i = 0; i < num_ol; ++i) {
        OrderLine *order_line = 
            s_order_line_tbl->GetPtr(readset[i].record.m_key);
        keys[1] = order_line->ol_i_id;
        uint64_t stock_key = TPCCKeyGen::create_stock_key(keys);
        dep_info.record.m_key = stock_key;
        m_level3_txn->readset.push_back(dep_info);
    }
    m_level3_txn->NowPhase();
    m_level3_txn->LaterPhase();
}

StockLevelTxn3::StockLevelTxn3(uint32_t warehouse_id, uint32_t district_id, 
                               int threshold) : 
    StockLevelTxn2(warehouse_id, district_id, threshold, this) {        
}

bool
StockLevelTxn3::NowPhase() {
    return true;
}

void
StockLevelTxn3::LaterPhase() {    
    m_stock_count = 0;
    uint32_t num_reads = readset.size();
    for (uint32_t i = 0; i < num_reads; ++i) {
        uint64_t stock_key = readset[i].record.m_key;
        Stock *stock = s_stock_tbl->GetPtr(stock_key);
        m_stock_count += 
            ((uint32_t)(stock->s_quantity - m_threshold)) >> 31;
    }
}

StockLevelTxn::StockLevelTxn(uint32_t warehouse_id, uint32_t district_id, 
                             int threshold) {
    assert(warehouse_id < s_num_warehouses);
    assert(district_id < s_districts_per_wh);

    m_threshold = threshold;
    m_stock_count = 0;
    m_warehouse_id = warehouse_id;
    m_district_id = district_id;
}

bool
StockLevelTxn::NowPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = ORDER_LINE;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;
    uint64_t district_key = TPCCKeyGen::create_district_key(keys);
    uint32_t next_order_id = s_district_tbl->GetPtr(district_key)->d_next_o_id;
    assert(next_order_id > 20);

    for (int i = 0; i < 20; ++i) {
        keys[2] = next_order_id - 1 - i;
        uint64_t oorder_key = TPCCKeyGen::create_order_key(keys);
        Oorder *open_order = s_oorder_tbl->GetPtr(oorder_key);
        if (open_order == NULL) {
            std::cout << "Warehouse: " << m_warehouse_id << "\nDistrict: ";
            std::cout << m_district_id << "\nOrder ID: " << keys[2] << "\n";
            assert(open_order != NULL);
        }
        for (uint32_t j = 0; j < open_order->o_ol_cnt; ++j) {
            keys[3] = j;
            dep_info.record.m_key = TPCCKeyGen::create_order_line_key(keys);
            readset.push_back(dep_info);
        }
    }
    return true;
}

void
StockLevelTxn::LaterPhase() {
    uint32_t keys[2];
    keys[0] = m_warehouse_id;
    uint32_t num_orderlines = readset.size();
    for (uint32_t i = 0; i < num_orderlines; ++i) {
        assert(readset[i].record.m_table == ORDER_LINE);
        uint64_t key = readset[i].record.m_key;
        OrderLine *ol = s_order_line_tbl->GetPtr(key);        
        uint32_t item_id = ol->ol_i_id;
        keys[1] = item_id;
        uint64_t stock_key = TPCCKeyGen::create_stock_key(keys);
        Stock *stock = s_stock_tbl->GetPtr(stock_key);
        m_stock_count += 
            ((uint32_t)(stock->s_quantity - m_threshold)) >> 31;
    }
}

OrderStatusTxn0::OrderStatusTxn0(uint32_t w_id, uint32_t d_id, uint32_t c_id, 
                                 char *c_last, bool c_by_name, 
                                 OrderStatusTxn1 *level1_txn) {
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);

    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = OPEN_ORDER_INDEX;    
    
    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_customer_id = c_id;
    m_c_by_name = c_by_name;
    m_c_last = c_last;
    m_level1_txn = level1_txn;
    
    uint32_t keys[3];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;
    keys[2] = m_customer_id;
    uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
    dep_info.record.m_key = customer_key;
    readset.push_back(dep_info);
}

bool
OrderStatusTxn0::NowPhase() {
    return true;
}

void
OrderStatusTxn0::LaterPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = ORDER_LINE;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;

    assert(readset[0].record.m_table == OPEN_ORDER_INDEX);
    uint64_t index = readset[0].record.m_key;
    Oorder *oorder = s_oorder_index->Get(index);
    keys[2] = oorder->o_id;
    for (uint32_t i = 0; i < oorder->o_ol_cnt; ++i) {
        keys[3] = i;
        uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
        dep_info.record.m_key = order_line_key;
        m_level1_txn->readset.push_back(dep_info);
    }
    m_level1_txn->NowPhase();
    m_level1_txn->LaterPhase();
}

OrderStatusTxn1::OrderStatusTxn1(uint32_t w_id, uint32_t d_id, uint32_t c_id, 
                                 char *c_last, bool c_by_name)
    : OrderStatusTxn0(w_id, d_id, c_id, c_last, c_by_name, this) {
}


bool
OrderStatusTxn1::NowPhase() {
    return true;
}

void
OrderStatusTxn1::LaterPhase() {
    int num_items = readset.size();
    for (int i = 0; i < num_items; ++i) {
        uint64_t order_line_key = readset[i].record.m_key;
        OrderLine *ol = s_order_line_tbl->GetPtr(order_line_key);
        m_order_line_quantity += ol->ol_quantity;
    }    
}

OrderStatusTxn::OrderStatusTxn(uint32_t w_id, uint32_t d_id, uint32_t c_id, 
                               char *c_last, bool c_by_name) {
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);
    
    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_customer_id = c_id;
    m_c_by_name = c_by_name;
    m_c_last = c_last;
}


bool
OrderStatusTxn::NowPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = ORDER_LINE;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;    
    keys[2] = m_customer_id;
    uint64_t cust_key = TPCCKeyGen::create_customer_key(keys);
    Oorder *open_order = s_oorder_index->Get(cust_key);
    keys[2] = open_order->o_id;
    for (uint32_t i = 0; i < open_order->o_ol_cnt; ++i) {
        keys[3] = i;
        uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
        dep_info.record.m_key = order_line_key;
        readset.push_back(dep_info);
    }
    return true;
}

void
OrderStatusTxn::LaterPhase() {    
    int num_items = readset.size();
    for (int i = 0; i < num_items; ++i) {
        uint64_t order_line_key = readset[i].record.m_key;
        OrderLine *ol = s_order_line_tbl->GetPtr(order_line_key);
        m_order_line_quantity += ol->ol_quantity;
    }
}

DeliveryTxn0::DeliveryTxn0(uint32_t w_id, uint32_t d_id, uint32_t carrier_id, 
                           DeliveryTxn1 *level1_txn) {
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);

    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_carrier_id = carrier_id;    
    m_level1_txn = level1_txn;
    m_open_order_ids = (uint32_t*)malloc(sizeof(uint32_t)*s_districts_per_wh);
    m_num_order_lines = (uint32_t*)malloc(sizeof(uint32_t)*s_districts_per_wh);
    memset(m_num_order_lines, 0, sizeof(uint32_t)*s_districts_per_wh);
    memset(m_open_order_ids, 0, sizeof(uint32_t)*s_districts_per_wh);
}

bool
DeliveryTxn0::NowPhase() {
    uint32_t keys[4];
    
    keys[0] = m_warehouse_id;
    for (uint32_t i = 0; i < s_districts_per_wh; ++i) {
        keys[1] = i;
        uint64_t district_key = TPCCKeyGen::create_district_key(keys);
        uint32_t *next_delivery = s_next_delivery_tbl->GetPtr(district_key);
        District *district = s_district_tbl->GetPtr(district_key);
        assert(next_delivery != NULL && district != NULL);
        if (district->d_next_o_id > *next_delivery) {
            m_open_order_ids[i] = *next_delivery;
            *next_delivery += 1;
        }
    }
    return true;
}

void
DeliveryTxn0::LaterPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = OPEN_ORDER;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;

    for (uint32_t i = 0; i < s_districts_per_wh; ++i) {
        keys[1] = i;
        if (m_open_order_ids[i] > 0) {
            keys[2] = m_open_order_ids[i];
            uint64_t open_order_key = TPCCKeyGen::create_order_key(keys);
            dep_info.record.m_key = open_order_key;
            m_level1_txn->writeset.push_back(dep_info);
        }
    }
    m_level1_txn->NowPhase();
    m_level1_txn->LaterPhase();
}

DeliveryTxn1::DeliveryTxn1(uint32_t w_id, uint32_t d_id, uint32_t carrier_id, 
                           DeliveryTxn2 *level2_txn) 
    : DeliveryTxn0(w_id, d_id, carrier_id, this) {
    m_level2_txn = level2_txn;
}

bool
DeliveryTxn1::NowPhase() {
    return true;
}

void
DeliveryTxn1::LaterPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    dep_info.record.m_table = OPEN_ORDER;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    uint32_t done = 0;
    
    uint32_t num_orders = writeset.size();
    
    for (uint32_t i = 0; i < num_orders; ++i) {
        Oorder *open_order = s_oorder_tbl->GetPtr(writeset[i].record.m_key);
        open_order->o_carrier_id = m_carrier_id;        
        keys[1] = open_order->o_d_id;

        // Add the customer who submitted this order to the writeset
        keys[2] = open_order->o_c_id;
        uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
        dep_info.record.m_table = CUSTOMER;
        dep_info.record.m_key = customer_key;
        m_level2_txn->writeset.push_back(dep_info);
        
        // Add the New Order record to the write set (this record is to be 
        // deleted)
        keys[2] = open_order->o_id;
        uint64_t open_order_key = TPCCKeyGen::create_new_order_key(keys);
        dep_info.record.m_table = NEW_ORDER;
        dep_info.record.m_key = open_order_key;
        m_level2_txn->writeset.push_back(dep_info);
        
        
        // Add all the order lines corresponding to this particular order to the 
        // readset
        for (uint32_t j = 0; j < open_order->o_ol_cnt; ++j) {
            keys[2] = open_order->o_id;
            keys[3] = j;
            uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
            dep_info.record.m_table = ORDER_LINE;
            dep_info.record.m_key = order_line_key;
            m_level2_txn->readset.push_back(dep_info);
        }
        done += open_order->o_ol_cnt;
        m_num_order_lines[i] = done;

    }
    m_level2_txn->NowPhase();
    m_level2_txn->LaterPhase();
}

DeliveryTxn2::DeliveryTxn2(uint32_t w_id, uint32_t d_id, uint32_t carrier_id) 
    : DeliveryTxn1(w_id, d_id, carrier_id, this) {
}


bool
DeliveryTxn2::NowPhase() {
    return true;
}

void
DeliveryTxn2::LaterPhase() {
    uint32_t done = 0;
    uint32_t write_count = writeset.size()/2;
    for (uint32_t i = 0; i < write_count; ++i) {
        uint32_t amount = 1;
        for (uint32_t j = done; j < m_num_order_lines[i]; ++j, ++done) {            
            assert(readset[j].record.m_table == ORDER_LINE);
            uint64_t order_line_key = readset[j].record.m_key;
            OrderLine *orderline = s_order_line_tbl->GetPtr(order_line_key);
            amount += orderline->ol_amount;
        }
        assert(writeset[2*i].record.m_table == CUSTOMER);
        uint64_t cust_key = writeset[2*i].record.m_key;
        Customer *cust = s_customer_tbl->GetPtr(cust_key);
        cust->c_balance += amount;
        cust->c_delivery_cnt += 1;
    }
}

DeliveryTxn::DeliveryTxn(uint32_t w_id, uint32_t d_id, uint32_t carrier_id) {
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);

    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_carrier_id = carrier_id;
}

bool
DeliveryTxn::NowPhase() {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    uint32_t done = 0;
    for (uint32_t i = 0; i < s_districts_per_wh; ++i) {
        keys[1] = i;
        uint64_t district_key = TPCCKeyGen::create_district_key(keys);
        uint32_t *next_delivery = s_next_delivery_tbl->GetPtr(district_key);
        District *district = s_district_tbl->GetPtr(district_key);
        assert(district->d_next_o_id >= *next_delivery);
        if (district->d_next_o_id > *next_delivery) {
            
            // Add the new order record to the writeset
            keys[2] = *next_delivery;
            uint64_t open_order_id = TPCCKeyGen::create_order_key(keys);
            //            dep_info.record.m_key = open_order_id;
            //            dep_info.record.m_table = NEW_ORDER;
            //            writeset.push_back(dep_info);
            
            // Add the order line items to the read set
            Oorder *oorder = s_oorder_tbl->GetPtr(open_order_id);
            assert(oorder != NULL);
            for (uint32_t j = 0; j < oorder->o_ol_cnt; ++j) {
                keys[3] = j;
                uint64_t order_line_key = 
                    TPCCKeyGen::create_order_line_key(keys);
                dep_info.record.m_table = ORDER_LINE;
                dep_info.record.m_key = order_line_key;
                readset.push_back(dep_info);
            }            
            done += oorder->o_ol_cnt;
            m_num_order_lines.push_back(done);            

            // Add the customer to the writeset            
            keys[2] = oorder->o_c_id;
            uint64_t customer_key = TPCCKeyGen::create_customer_key(keys);
            dep_info.record.m_key = customer_key;
            dep_info.record.m_table = CUSTOMER;
            writeset.push_back(dep_info);

            // Write to the open order and district tables
            oorder->o_carrier_id = m_carrier_id;
            *next_delivery += 1;
        }
    }
    return true;
}

void
DeliveryTxn::LaterPhase() {
    int done = 0;
    uint32_t write_count = writeset.size();
    for (uint32_t i = 0; i < write_count; ++i) {
        uint32_t amount = 0;
        int num_order_lines = m_num_order_lines[i];
        for (int j = done; j < num_order_lines; ++j, ++done) {
            assert(readset[j].record.m_table == ORDER_LINE);
            uint64_t order_line_key = readset[j].record.m_key;
            OrderLine *orderline = s_order_line_tbl->GetPtr(order_line_key);
            amount += orderline->ol_amount;
        }
        assert(writeset[i].record.m_table == CUSTOMER);
        uint64_t cust_key = writeset[i].record.m_key;
        Customer *cust = s_customer_tbl->GetPtr(cust_key);
        cust->c_balance += amount;
        cust->c_delivery_cnt += 1;
    }
}

