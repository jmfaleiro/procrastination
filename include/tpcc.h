// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// Adapted from oltpbench (git@github.com:oltpbenchmark/oltpbench.git)

#include <stdint.h>

#include <string>
#include <vector>

#include <table.hh>
#include <keys.h>
#include <action.h>

namespace tpcc {

  enum TPCCTable {
    WAREHOUSE = 0,
    DISTRICT,
    CUSTOMER,
    HISTORY,
    NEW_ORDER,
    ORDER,
    ORDER_LINE,
    ITEM,
    STOCK,    
  };
  
  class TPCCKeyGen {
  private:
    static const uint32_t s_customer_shift = 		24;
    static const uint32_t s_district_shift = 		16;
    static const uint32_t s_new_order_shift = 		24;
    static const uint32_t s_order_shift = 		24;
    static const uint32_t s_order_line_shift = 		56;
    static const uint32_t s_stock_shift = 		16;

    static const uint64_t s_customer_mask = 		0x000000FFFF000000;
    static const uint64_t s_district_mask = 		0x0000000000FF0000;
    static const uint64_t s_warehouse_mask = 		0x000000000000FFFF;
    static const uint64_t s_stock_mask = 		0x00000000000000FF;


  public:
    static inline uint32_t
    get_stock_key(uint64_t composite_key) {
      return (uint32_t)((composite_key & s_stock_mask) >> s_stock_shift);
    }

    static inline uint32_t
    get_customer_key(uint64_t composite_key) {
      return (uint32_t)((composite_key & s_customer_mask) >> s_customer_shift);
    }

    static inline uint32_t
    get_warehouse_key(uint64_t composite_key) {
      return (uint32_t)(composite_key & s_warehouse_mask);
    }

    static inline uint32_t
    get_district_key(uint64_t composite_key) {
      return (uint32_t)((composite_key & s_district_mask) >> s_district_shift);
    }

    // Expects: 	warehouse_id 	==> 	keys[0] 
    // 		district_id  	==> 	keys[1]
    //		customer_id  	==> 	keys[2]
    static inline uint64_t
    create_customer_key(uint32_t *keys) {
      return (
              ((uint64_t)keys[0])				|
              ((uint64_t)keys[1] << s_district_shift)		|
              ((uint64_t)keys[2] << s_customer_shift)		
              );
    }

    // Expects: 	warehouse_id 	==> 	keys[0]
    //		district_id 	==> 	keys[1]
    static inline uint64_t
    create_district_key(uint32_t *keys) {
      return (
              ((uint64_t)keys[0])				|
              ((uint64_t)keys[1] << s_district_shift)
              );
    }

    // Expects: 	warehouse_id 	==> 	keys[0]
    // 		district_id 	==> 	keys[1]
    //		new_order_id	==> 	keys[2]
    static inline uint64_t
    create_new_order_key(uint32_t *keys) {
      return (
              ((uint64_t)keys[0])    				|
              ((uint64_t)keys[1] << s_district_shift)		|
              ((uint64_t)keys[2] << s_new_order_shift)
              );
    }	

    // Expects: 	warehouse_id 	==> 	keys[0]
    // 		district_id 	==> 	keys[1]
    //		order_id	==> 	keys[2]
    static inline uint64_t
    create_order_key(uint32_t *keys) {
      return (
              ((uint64_t)keys[0])				|
              ((uint64_t)keys[1] << s_district_shift)		|
              ((uint64_t)keys[2] << s_order_shift)
              );
    }

    static inline uint64_t
    create_order_line_key(uint32_t *keys) {
      return (
              ((uint64_t)keys[0]) 				| 
              ((uint64_t)keys[1] << s_district_shift) 	| 
              ((uint64_t)keys[2] << s_order_shift)		|
              ((uint64_t)keys[3] << s_order_line_shift)
              );
    }

    static inline uint64_t
    create_stock_key(uint32_t *keys) {
      return (
              ((uint64_t)keys[0])				|
              ((uint64_t)keys[1] << s_stock_shift)		
              );
    }

  };

  // Each of the following classes defines a TPC-C table. 
  typedef struct {
    int c_id;
    int c_d_id;
    int c_w_id;
    int c_payment_cnt;
    int c_delivery_cnt;
    std::string c_since;
    float c_discount;
    float c_credit_lim;
    float c_balance;
    float c_ytd_payment;
    std::string c_credit;
    std::string c_last;
    std::string c_first;
    std::string c_street_1;
    std::string c_street_2;
    std::string c_city;
    std::string c_state;
    std::string c_zip;
    std::string c_phone;
    std::string c_middle;
    std::string c_data;  
  } Customer;

  typedef struct {
    int d_id;
    int d_w_id;
    int d_next_o_id;
    float d_ytd;
    float d_tax;
    std::string d_name;
    std::string d_street_1;
    std::string d_street_2;
    std::string d_city;
    std::string d_state;
    std::string d_zip;
    Customer* customer_table;
  } District;

  typedef struct {
    int h_c_id;
    int h_c_d_id;
    int h_c_w_id;
    int h_d_id;
    int h_w_id;
    std::string h_date;
    float h_amount;
    std::string h_data;
  } History;

  typedef struct {
    int i_id; // PRIMARY KEY
    int i_im_id;
    float i_price;
    std::string i_name;
    std::string i_data;
  } Item;

  typedef struct {
    int no_w_id;
    int no_d_id;
    int no_o_id;
  } NewOrder;

  typedef struct {
    int o_id;
    int o_w_id;
    int o_d_id;
    int o_c_id;
    int o_carrier_id;
    int o_ol_cnt;
    int o_all_local;
    long o_entry_d;
  } Oorder;

  typedef struct { 
    int ol_w_id;
    int ol_d_id;
    int ol_o_id;
    int ol_number;
    int ol_i_id;
    int ol_supply_w_id;
    int ol_quantity;
    long ol_delivery_d;
    float ol_amount;
    std::string ol_dist_info;
  } OrderLine;

  typedef struct {
    int s_i_id; // PRIMARY KEY 2
    int s_w_id; // PRIMARY KEY 1
    int s_order_cnt;
    int s_remote_cnt;
    int s_quantity;
    float s_ytd;
    std::string s_data;
    std::string s_dist_01;
    std::string s_dist_02;
    std::string s_dist_03;
    std::string s_dist_04;
    std::string s_dist_05;
    std::string s_dist_06;
    std::string s_dist_07;
    std::string s_dist_08;
    std::string s_dist_09;
    std::string s_dist_10;
  } Stock;

  typedef struct {
    int w_id; // PRIMARY KEY
    float w_ytd;
    float w_tax;
    std::string w_name;
    std::string w_street_1;
    std::string w_street_2;
    std::string w_city;
    std::string w_state;
    std::string w_zip;
    District *district_table;
    Stock *stock_table;
  } Warehouse;
  
  static Customer 				*s_customer_tbl;
  static District 				*s_district_tbl;
  static Warehouse 				*s_warehouse_tbl;
  static Item 					*s_item_tbl;
  
  static Table<uint64_t, NewOrder> 		*s_new_order_tbl;
  static Table<uint64_t, Oorder> 		*s_oorder_tbl;
  static Table<uint64_t, OrderLine> 		*s_order_line_tbl;
  static Table<uint64_t, Stock> 		*s_stock_tbl;

  class NewOrderTxn : public Action {
    
  private:
    // Read set indices
    static const uint32_t s_customer_index = 0;
    static const uint32_t s_warehouse_index = 1;
    static const uint32_t s_item_index = 2;

    // Write set indices
    static const uint32_t s_district_index = 0;
    static const uint32_t s_stock_index = 1;

    static const uint64_t invalid_item_key = 0xFFFFFFFFFFFFFFFF;

    // Fields 
    int m_order_id;
    int m_district_id;
    int m_warehouse_id;
    float m_district_tax;
    int *m_order_quantities;
    int *m_supplierWarehouse_ids;
    int m_num_items;

  public:
    NewOrderTxn(int w_id, int d_id, int c_id, int o_ol_cnt, 
                int o_all_local, int numItems, int *itemIds, 
                int *supplierWarehouseIDs, int *orderQuantities);
    bool NowPhase();
    void LaterPhase();
  };
}; // tpcc namespace. 

