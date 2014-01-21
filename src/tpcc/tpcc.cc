#include <tpcc/tpcc.h>
#include <string>
#include <sstream>

using namespace std;

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
tpcc::NewOrder::NewOrder(int w_id, int d_id, int c_id, int o_ol_cnt, 
                         int o_all_local, int numItems, int *itemIds, 
                         int *supplierWarehouseIDs, int *orderQuantities) {



  ostringstream os;

  // Create the customer composite key. 
  os << w_id << "###" << d_id << "###" << c_id;
  string customer_key = os.str();
  os.str(std::string());  
  string cust_composite_key = CreateKey("cust", customer_key);
  m_read_set.push_back(cust_composite_key);

  // Create the warehouse composite key. 
  os << w_id;
  string warehouse_key = os.str();
  os.str(std::string());
  string wh_composite_key = CreateKey("warehouse", warehouse_key);
  m_read_set.push_back(wh_composite_key);

  // Create the district key. 
  os << w_id << "###" << d_id;
  string district_key = os.str();
  os.str(std::string());
  string district_composite_key = CreateKey("district", district_key);
  m_write_set.push_back(district_composite_key);

  // Create stock, and item keys for each item. 
  for (int i = 0; i < numItems; ++i) {
    os << itemIds[i] << "###" << supplierWarehouseIDs;
    string stock_key = os.str();
    os.str(std::string());
    string stock_composite_key = CreateKey("stock", stock_key);
    m_write_set.push_back(stock_composite_key);

    os << itemIds[i];
    string item_key = os.str();
    os.str(std::string());
    string item_composite_key = CreateKey("item", item_key);
    m_read_set.push_back(item_composite_key);
  }
}

bool
tpcc::NewOrder::NowPhase() {

  // A NewOrder txn aborts if any of the item keys are invalid. 
  int num_items = m_read_set.size();
  for (int i = s_item_index; i < num_items; ++i) {

    // Make sure that all the items requested exist. 
    string item_key = GetPKey(m_read_set[i]);
    if (s_item_tbl.count(item_key) == 0) {
      return false;		// Abort the txn. 
    }
  }
  
  // We are guaranteed not to abort once we're here. Increment the highly 
  // contended next_order_id in the district table.
  string district_key = GetPKey(m_write_set[s_district_index]);
  District *district = s_district_tbl[district_key];
  m_order_id = district->d_next_o_id;
  district->d_next_o_id += 1;
  return true;		// The txn can be considered committed. 
}

void
tpcc::NewOrder::LaterPhase() {

}
