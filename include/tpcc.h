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
        // 				district_id  	==> 	keys[1]
        //				customer_id  	==> 	keys[2]
        static inline uint64_t
            create_customer_key(uint32_t *keys) {
            return (
                    ((uint64_t)keys[0])				|
                    ((uint64_t)keys[1] << s_district_shift)		|
                    ((uint64_t)keys[2] << s_customer_shift)		
                    );
        }

        // Expects: 	warehouse_id 	==> 	keys[0]
        //			district_id 	==> 	keys[1]
        static inline uint64_t
            create_district_key(uint32_t *keys) {
            return (
                    ((uint64_t)keys[0])				|
                    ((uint64_t)keys[1] << s_district_shift)
                    );
        }

        // Expects: 	warehouse_id 	==> 	keys[0]
        // 			district_id 	==> 	keys[1]
        //			new_order_id	==> 	keys[2]
        static inline uint64_t
            create_new_order_key(uint32_t *keys) {
            return (
                    ((uint64_t)keys[0])    				|
                    ((uint64_t)keys[1] << s_district_shift)		|
                    ((uint64_t)keys[2] << s_new_order_shift)
                    );
        }	

        // Expects: 	warehouse_id 	==> 	keys[0]
        // 			district_id 	==> 	keys[1]
        //			order_id	==> 	keys[2]
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
        char c_since[25];
        float c_discount;
        float c_credit_lim;
        float c_balance;
        float c_ytd_payment;
        char c_credit[25];
        char c_last[25];
        char c_first[25];
        char c_street_1[25];
        char c_street_2[25];
        char c_city[25];
        char c_state[5];
        char c_zip[10];
        char c_phone[20];
        char c_middle[5];
        char c_data[501];  
    } Customer;

    typedef struct {
        int d_id;
        int d_w_id;
        int d_next_o_id;
        float d_ytd;
        float d_tax;
        char d_name[12];
        char d_street_1[25];
        char d_street_2[25];
        char d_city[25];
        char d_state[5];
        char d_zip[5];
        Customer* d_customer_table;
    } District __attribute((aligned(CACHE_LINE)));

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
        char i_name[25];
        char i_data[55];
    } Item;

    typedef struct {
        int no_w_id;
        int no_d_id;
        int no_o_id;
    } NewOrder;

    typedef struct {
        uint32_t o_id;
        uint32_t o_w_id;
        uint32_t o_d_id;
        uint32_t o_c_id;
        uint32_t o_carrier_id;
        uint32_t o_ol_cnt;
        uint32_t o_all_local;
        long o_entry_d;
    } Oorder;

    typedef struct { 
        uint32_t ol_w_id;
        uint32_t ol_d_id;
        uint32_t ol_o_id;
        uint32_t ol_number;
        uint32_t ol_i_id;
        uint32_t ol_supply_w_id;
        uint32_t ol_quantity;
        long ol_delivery_d;
        float ol_amount;
        char ol_dist_info[25];
    } OrderLine;

    typedef struct {
        int s_i_id; // PRIMARY KEY 2
        int s_w_id; // PRIMARY KEY 1
        int s_order_cnt;
        int s_remote_cnt;
        int s_quantity;
        float s_ytd;
        char s_data[25];
        char s_dist_01[25];
        char s_dist_02[25];
        char s_dist_03[25];
        char s_dist_04[25];
        char s_dist_05[25];
        char s_dist_06[25];
        char s_dist_07[25];
        char s_dist_08[25];
        char s_dist_09[25];
        char s_dist_10[25];
    } Stock __attribute__((aligned(CACHE_LINE)));

    typedef struct {
        int w_id; // PRIMARY KEY
        float w_ytd;
        float w_tax;
        char w_name[11];
        char w_street_1[25];
        char w_street_2[25];
        char w_city[25];
        char w_state[4];
        char w_zip[10];
        District *w_district_table;
        Stock *w_stock_table;
    } Warehouse;
  
    
    static Warehouse 						*s_warehouse_tbl;
    static Item 							*s_item_tbl;
  
    static Table<uint64_t, NewOrder> 		*s_new_order_tbl;
    static Table<uint64_t, Oorder> 			*s_oorder_tbl;
    static Table<uint64_t, OrderLine> 		*s_order_line_tbl;
    

    class TPCCInit {
    private:
        uint32_t m_num_warehouses;
        uint32_t m_dist_per_wh;
        uint32_t m_cust_per_dist;
        uint32_t m_item_count;
        
        uint32_t s_first_unprocessed_o_id;
        // Generate a random string of specified length (all chars are assumed to be
        // lower case).
        void gen_random_string(int min_len, int max_len, char *val);
        // Each of the functions below initializes an apriori allocated single row.
        void init_warehouse(Warehouse *wh);
        void init_district(District *district, uint32_t warehouse_id);
        void init_customer(Customer *customer, uint32_t d_id, uint32_t w_id);
        void init_history(History *history);
        void init_order();
        void init_item(Item *item);
        void init_stock(Stock *stock, uint32_t wh_id);

    public:
        TPCCInit(uint32_t num_warehouses, uint32_t dist_per_wh, 
                 uint32_t cust_per_dist, uint32_t item_count);

        // Must be called before running any experiments. 
        void do_init();
    };


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

