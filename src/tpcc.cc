// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// Adapted from oltpbench (git@github.com:oltpbenchmark/oltpbench.git)

#include <tpcc.h>
#include <string>
#include <sstream>

using namespace std;

static inline uint32_t
tpcc::TPCCKeyGen::get_customer_key(uint64_t composite_key) {
  return (uint32_t)((composite_key & s_customer_mask) >> s_customer_shift);
}

static inline uint32_t
tpcc::TPCCKeyGen::get_warehouse_key(uint64_t composite_key) {
  return (uint32_t)(composite_key & s_warehouse_mask);
}

static inline uint32_t
tpcc::TPCCKeyGen::get_district_key(uint64_t composite_key) {
  return (uint32_t)((composite_key & s_district_mask) >> s_district_shift);
}

// Expects: 	warehouse_id 	==> 	keys[0] 
// 		district_id  	==> 	keys[1]
//		customer_id  	==> 	keys[2]
static inline uint64_t
tpcc::TPCCKeyGen::create_customer_key(uint32_t *keys) {
  return (
          (keys[0])				|
          (keys[1] << s_district_shift)		|
          (keys[2] << s_customer_shift)		|
         );
}

// Expects: 	warehouse_id 	==> 	keys[0]
//		district_id 	==> 	keys[1]
static inline uint64_t
tpcc::TPCCKeyGen::create_district_key(uint32_t *keys) {
  return (
          (keys[0])				|
          (keys[1] << s_district_shift)
         );
}

// Expects: 	warehouse_id 	==> 	keys[0]
// 		district_id 	==> 	keys[1]
//		new_order_id	==> 	keys[2]
static inline uint64_t
tpcc::TPCCKeyGen::create_new_order_key(uint32_t *keys) {
  return (
          (keys[0])    				|
          (keys[1] << s_district_shift)		|
          (keys[2] << s_new_order_shift)
         );
}	

// Expects: 	warehouse_id 	==> 	keys[0]
// 		district_id 	==> 	keys[1]
//		order_id	==> 	keys[2]
static inline uint64_t
tpcc::TPCCKeyGen::create_order_key(uint32_t *keys) {
  return (
          (keys[0])				|
          (keys[1] << s_district_shift)		|
          (keys[2] << s_order_shift)
         );
}

static inline uint64_t
tpcc::TPCCKeyGen::create_order_line_key(uint32_t *keys) {
  return (
          (keys[0]) 				| 
          (keys[1] << s_district_shift) 	| 
          (keys[2] << s_order_shift)		|
          (keys[3] << s_order_line_shift)
         );
}

static inline uint64_t
tpcc::TPCCKeyGen::create_stock_key(uint32_t *keys) {
  return (
          (keys[0])				|
          (keys[1] << s_stock_shift)		|
         );
}

// Append table identifier to the primary key. Use this string to identify 
// records in a transaction's read/write sets. 
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
tpcc::NewOrderTxn::NewOrderTxn(int w_id, int d_id, int c_id, int o_ol_cnt, 
                               int o_all_local, int numItems, int *itemIds, 
                               int *supplierWarehouseIDs, 
                               int *orderQuantities) {

  uint32_t keys[10];
  CompositeKey composite;
  uint64_t table_id;

  // Create the warehouse, customer, and district keys. 
  keys[0] = w_id;
  keys[1] = d_id;
  keys[2] = c_id;
  uint64_t warehouse_key = w_id;
  uint64_t customer_key = create_customer_key(keys);
  uint64_t district_key = create_district_key(keys);
  
  // Insert the customer key in the read set. 
  composite.table = CUSTOMER;
  composite.key = customer_key;
  m_read_set.push_back(composite);
  
  // Insert the warehouse key in the read set. 
  composite.table = WAREHOUSE;
  composite.key = warehouse_key;
  m_read_set.push_back(composite);
  
  // Insert the district key in the write set.
  composite.table = DISTRICT;
  composite.key = district_key;
  m_write_set.push(composite);

  // Create stock, and item keys for each item. 
  for (int i = 0; i < numItems; ++i) {

    // Create the item and stock keys. 
    uint64_t item_key = itemIds[i];
    keys[0] = supplierWarehouseIDs[i];
    keys[1] = itemIds[i];
    uint64_t stock_key = create_stock_key(keys);


    // Insert the item key into the read set. 
    composite.key = item_key;
    composite.table = ITEM;
    m_read_set.push(composite);

    // Insert the stock key into the write set. 
    composite.key = stock_key;
    composite.table = STOCK;
    m_write_set.push(composite);
  }
}

bool
tpcc::NewOrderTxn::NowPhase() {
  
  CompositeKey composite;

  // A NewOrder txn aborts if any of the item keys are invalid. 
  int num_items = m_read_set.size();
  for (int i = s_item_index; i < num_items; ++i) {

    // Make sure that all the items requested exist. 
    composite = m_read_set[i];
    uint64_t item_key = composite.key;
    if (!s_item_tbl.Exists(item_key)) {
      return false;	// Abort the txn. 
    }
  }
  
  // We are guaranteed not to abort once we're here. Increment the highly 
  // contended next_order_id in the district table.
  composite = m_write_set[s_district_index];
  uint64_t district_key = get_district_key(composite.key);
  uint64_t warehouse_key = get_warehouse_key(composite.key);  
  District *district_tbl = s_warehouse_tbl[warehouse_key].district_table;
  District *district = &district_tbl[district_key];
  
  // Update the district record. 
  m_order_id = district->d_next_o_id;
  m_district_tax = district->d_tax;
  district->d_next_o_id += 1;
  return true;		// The txn can be considered committed. 
}

void
tpcc::NewOrderTxn::LaterPhase() {
  float total_amount = 0;
  CompositeKey composite;
  uint32_t keys[5];
  ostringstream os; 
  
  // Read the customer record.
  composite = m_read_set[s_customer_index];
  uint64_t w_id = get_warehouse_key(composite.key);
  uint64_t d_id = get_district_key(composite.key);
  uint64_t c_id = get_customer_key(composite.key);
  
  Customer *customer = 
    &(s_warehouse_tbl[w_id].district_table[d_id].customer_table[c_id]);

  string c_last = customer->c_last;
  float c_discount = customer->c_discount;
  string c_credit = customer->c_credit;

  // Read the warehouse record.
  Warehouse *warehouse = s_warehouse_tbl[w_id];
  float w_tax = warehouse->w_tax;  

  // Create a NewOrder record. 
  // XXX: This is a potentially serializing call to malloc. Make sure to link
  // TCMalloc so that allocations can be (mostly) thread-local. 
  NewOrder *new_order_record = (NewOrder*)malloc(sizeof(NewOrder));
  new_order_record->no_w_id = w_id;
  new_order_record->no_d_id = d_id;
  new_order_record->no_o_id = m_order_id;
  keys[0] = w_id;
  keys[1] = d_id;
  keys[2] = m_order_id;

  // Generate the NewOrder key and insert the record.
  // XXX: This insertion has to be concurrent. Therefore, use a hash-table to 
  // implement it. 
  uint64_t no_id = create_new_order_key(keys);
  s_new_order_tbl->Put(no_id, new_order_record);
  
  for (int i = 0; i < m_num_items; ++i) {
    composite = m_write_set[s_stock_index+i];
    uint64_t ol_s_id = get_stock_key(composite.key);
    uint64_t ol_w_id = get_warehouse_key(composite.key);
    uint64_t ol_i_id = m_write_set[s_item_index+i].key;
    int ol_quantity = m_order_quantities[i];
    
    // Get the item and the stock records. 
    Item *item = s_item_tbl->Get(ol_i_id);
    Stock *stock = &(s_warehouse_tbl[ol_w_id].stock_table[ol_s_id]);
    
    // Update the inventory for the item in question. 
    if (stock->s_order_cnt - quantity >= 10) {
      stock->s_quantity -= quantity;
    }
    else {
      stock->s_quantity += -quantity + 91;
    }    
    if (ol_w_id != w_id) {
      stock->s_remote_cnt += 1;
    }
    stock->s_ytd += quantity;
    total_amount += ol_quantity * (item->i_price);
    
    string ol_dist_info;
    switch (m_d_id) {
    case 1:
      ol_dist_info = stock->s_dist_01;
      break;
    case 2:
      ol_dist_info = stock->s_dist_02;
      break;
    case 3:
      ol_dist_info = stock->s_dist_03;
      break;
    case 4:
      ol_dist_info = stock->s_dist_04;
      break;
    case 5:
      ol_dist_info = stock->s_dist_05;
      break;
    case 6:
      ol_dist_info = stock->s_dist_06;
      break;
    case 7:
      ol_dist_info = stock->s_dist_07;
      break;
    case 8:
      ol_dist_info = stock->s_dist_08;
      break;
    case 9:
      ol_dist_info = stock->s_dist_09;
      break;
    case 10:
      ol_dist_info = stock->s_dist_10;
      break;
    default:
      assert(false);
    }
    
    // Finally, insert an item into the order line table.
    // XXX: This memory allocation can serialize threads. Link TCMalloc to 
    // ensure that all allocations happen with minimal possible kernel-level 
    // synchronization.
    OrderLine *new_order_line = (OrderLine*)malloc(sizeof(OrderLine));
    new_order_line->ol_o_id = m_order_id;
    new_order_line->ol_d_id = d_id;
    new_order_line->ol_w_id = w_id;
    new_order_line->ol_number = i + 1;
    new_order_line->ol_i_id = item->i_id;
    new_order_line->ol_supply_w_id = ol_w_id;
    new_order_line->ol_quantity = m_order_quantities[i];
    new_order_line->ol_amount = m_order_quantities[i] * (item->i_price);
    new_order_line->ol_dist_info = ol_dist_info;
    
    keys[0] = w_id;
    keys[1] = d_id;
    keys[2] = m_order_id;
    keys[3] = i+1;
    uint64_t order_line_key = create_order_line_key(keys);

    // XXX: Make sure that the put operation is implemented on top of a 
    // concurrent hash table. 
    s_order_line_tbl->Put(order_line_key, new_order_line);
  }
}


/* Read the district table 			<w_id, d_id> 
 * Read the Stock table 			<s_w_id, s_i_id>
 * Read the Orderline table 		<ol_w_id, ol_d_id, ol_o_id, ol_number>
 *
 * Read set: 	District key
 *				Orderline key + Stock key (20 records)
 */
/*
tpcc::StockLevelTxn::StockLevelTxn(int w_id, int d_id, int threshold) {
  ostringsrream os;
  os << w_id << "###" << d_id;
  string district_key = os.str();
  os.str(std::string());
  
  
}

bool
tpcc::StockLevelTxn::NowPhase() {
  return false;
}

void
tpcc::StockLevelTxn::LaterPhase() {
  
}
*/
