#include <storage/hash_store.h>

namespace tpcc {

  // Each of the following classes defines a TPC-C table. 
  typedef struct {
    public int c_id;
    public int c_d_id;
    public int c_w_id;
    public int c_payment_cnt;
    public int c_delivery_cnt;
    public string c_since;
    public float c_discount;
    public float c_credit_lim;
    public float c_balance;
    public float c_ytd_payment;
    public string c_credit;
    public string c_last;
    public string c_first;
    public string c_street_1;
    public string c_street_2;
    public string c_city;
    public string c_state;
    public string c_zip;
    public string c_phone;
    public string c_middle;
    public string c_data;  
  } Customer;

  typedef struct {
    public int d_id;
    public int d_w_id;
    public int d_next_o_id;
    public float d_ytd;
    public float d_tax;
    public string d_name;
    public string d_street_1;
    public string d_street_2;
    public string d_city;
    public string d_state;
    public string d_zip;
  } District;

  typedef struct {
    public int h_c_id;
    public int h_c_d_id;
    public int h_c_w_id;
    public int h_d_id;
    public int h_w_id;
    public string h_date;
    public float h_amount;
    public string h_data;
  } History;

  typedef struct {
    public int i_id; // PRIMARY KEY
    public int i_im_id;
    public float i_price;
    public String i_name;
    public String i_data;
  } Item;

  typedef struct {
    public int no_w_id;
    pubilc int no_d_id;
    public int no_o_id;
  } NewOrder;

  typedef struct {
    public int o_id;
    public int o_w_id;
    public int o_d_id;
    public int o_c_id;
    public Integer o_carrier_id;
    public int o_ol_cnt;
    public int o_all_local;
    public long o_entry_d;
  } Oorder;

  typedef struct { 
    public int ol_w_id;
    public int ol_d_id;
    public int ol_o_id;
    public int ol_number;
    public int ol_i_id;
    public int ol_supply_w_id;
    public int ol_quantity;
    public Long ol_delivery_d;
    public float ol_amount;
    public String ol_dist_info;
  } OrderLine;

  typedef struct {
    public int s_i_id; // PRIMARY KEY 2
    public int s_w_id; // PRIMARY KEY 1
    public int s_order_cnt;
    public int s_remote_cnt;
    public int s_quantity;
    public float s_ytd;
    public String s_data;
    public String s_dist_01;
    public String s_dist_02;
    public String s_dist_03;
    public String s_dist_04;
    public String s_dist_05;
    public String s_dist_06;
    public String s_dist_07;
    public String s_dist_08;
    public String s_dist_09;
    public String s_dist_10;
  } Stock;

  typedef struct {
    public int w_id; // PRIMARY KEY
    public float w_ytd;
    public float w_tax;
    public String w_name;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public String w_zip;
  } Warehouse;
  
  // Use this enum to disambiguate various types of TPCC transactions. 
  typedef enum TxnType {
    DELIVERY,
    NEW_ORDER,
    ORDER_STATUS,
    PAYMENT,
    STOCK_LEVEL,    
  };
  
  static HashStore<string, Customer> 	*s_customer_tbl;
  static HashStore<string, District> 	*s_district_tbl;
  static HashStore<string, History> 	*s_history_tbl;
  static HashStore<string, Item> 		*s_item_tbl;
  static HashStore<string, NewOrder> 	*s_new_order_tbl;
  static HashStore<string, Oorder> 		*s_oorder_tbl;
  static HashStore<string, OrderLine> 	*s_order_line_tbl;
  static HashStore<string, Stock> 		*s_stock_tbl;
  static HashStore<string, Warehouse> 	*s_warehouse_tbl;


  class NewOrder : public ILazyTxn {
    
  private:
    // Read set indices
    static const s_customer_index = 0;
    static const s_warehouse_index = 1;
    static const s_item_index = 2;

    // Write set indices
    static const s_district_index = 0;

    // Remember the order id so we can use it in the later-phase. 
    int m_order_id;

  public:
    NewOrder(int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local,
             vector<int> *itemIds, vector<int> *supplierWarehouseIDs, 
             vector<int> *orderQuantities);

    bool NowPhase();
    void LaterPhase();
    vector<string> getReadSet();
    vector<string> getWriteSet();
  };

  // Implement a single "run" function which runs a particular TPCC function. 
  class Worker {
  private:
    HashStore<string, Customer> 	*m_customer_tbl;
    HashStore<string, District> 	*m_district_tbl;
    HashStore<string, History> 		*m_history_tbl;
    HashStore<string, Item> 		*m_item_tbl;
    HashStore<string, NewOrder> 	*m_new_order_tbl;
    HashStore<string, Oorder> 		*m_oorder_tbl;
    HashStore<string, OrderLine> 	*m_order_line_tbl;
    HashStore<string, Stock> 		*m_stock_tbl;
    HashStore<string, Warehouse> 	*m_warehouse_tbl;

    void NewOrder(int w_id, int d_id, int c_id,
                  int o_ol_cnt, int o_all_local, int[] itemIDs,
                  int[] supplierWarehouseIDs, int[] orderQuantities);
    
    

  public:
    TPCCWorker(HashStore<string, Customer> 	*customer_tbl;
               HashStore<string, District> 	*district_tbl;
               HashStore<string, History> 	*history_tbl;
               HashStore<string, Item> 		*item_tbl;
               HashStore<string, NewOrder> 	*new_order_tbl;
               HashStore<string, Oorder> 	*oorder_tbl;
               HashStore<string, OrderLine> *order_line_tbl;
               HashStore<string, Stock> 	*stock_tbl;
               HashStore<string, Warehouse> *warehouse_tbl);

  };

}; // tpcc namespace. 

