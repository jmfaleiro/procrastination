// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// Adapted from oltpbench (git@github.com:oltpbenchmark/oltpbench.git)

#ifndef TPCC_H_
#define TPCC_H_

#include <stdint.h>

#include <string>
#include <vector>

#include <concurrent_hash_table.hh>
#include <string_table.hh>
#include <keys.h>
#include <action.h>

enum TPCCTable {
    WAREHOUSE = 0,
    DISTRICT,
    CUSTOMER,
    HISTORY,
    NEW_ORDER,
    OPEN_ORDER,
    ORDER_LINE,
    ITEM,
    STOCK,    
    ORDER_LINE_INDEX,
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
    static const uint64_t s_stock_mask = 			0xFFFFFFFFFFFF0000;


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
                (((uint64_t)keys[1]) << s_district_shift)		|
                (((uint64_t)keys[2]) << s_customer_shift)		
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
                (((uint64_t)keys[1]) << s_district_shift)		|
                (((uint64_t)keys[2]) << s_order_shift)
                );
    }

    static inline uint64_t
    create_order_line_key(uint32_t *keys) {            
        return (
                ((uint64_t)keys[0]) 				| 
                (((uint64_t)keys[1]) << s_district_shift) 	| 
                (((uint64_t)keys[2]) << s_order_shift)		|
                (((uint64_t)keys[3]) << s_order_line_shift)
                );
    }

    static inline uint64_t
    create_stock_key(uint32_t *keys) {
        return (
                ((uint64_t)keys[0])				|
                (((uint64_t)keys[1]) << s_stock_shift)		
                );
    }

};

// Each of the following classes defines a TPC-C table. 
typedef struct {
    uint32_t	c_id;
    uint32_t	c_d_id;
    uint32_t	c_w_id;
    int 		c_payment_cnt;
    int 		c_delivery_cnt;
    char 		*c_since;
    float 		c_discount;
    float 		c_credit_lim;
    float 		c_balance;
    float 		c_ytd_payment;
    char 		c_credit[3];
    char 		c_last[16];
    char 		c_first[17];
    char 		c_street_1[21];
    char 		c_street_2[21];
    char 		c_city[21];
    char 		c_state[3];
    char 		c_zip[11];
    char 		c_phone[17];
    char 		c_middle[3];
    char 		c_data[501];
} Customer;

typedef struct {
    int 		d_id;
    int 		d_w_id;
    uint32_t	d_next_o_id;
    float 		d_ytd;
    float 		d_tax;
    char 		d_name[11];
    char 		d_street_1[21];
    char 		d_street_2[21];
    char 		d_city[21];
    char 		d_state[4];
    char 		d_zip[10];
} District __attribute((aligned(CACHE_LINE)));

typedef struct {
    uint32_t	h_c_id;
    uint32_t	h_c_d_id;
    uint32_t	h_c_w_id;
    uint32_t	h_d_id;
    uint32_t	h_w_id;
    uint32_t 	h_date;
    float 		h_amount;
    char		h_data[26];
} History;

typedef struct {
    int 		i_id; // PRIMARY KEY
    int 		i_im_id;
    float 		i_price;
    char 		i_name[25];
    char 		i_data[51];
} Item;

typedef struct {
    int 		no_w_id;
    int 		no_d_id;
    int 		no_o_id;
} NewOrder;

typedef struct {
    uint32_t 	o_id;
    uint32_t 	o_w_id;
    uint32_t 	o_d_id;
    uint32_t 	o_c_id;
    uint32_t 	o_carrier_id;
    uint32_t 	o_ol_cnt;
    uint32_t 	o_all_local;
    long 		o_entry_d;
} Oorder;

typedef struct { 
    uint32_t 	ol_w_id;
    uint32_t 	ol_d_id;
    uint32_t 	ol_o_id;
    uint32_t 	ol_number;
    uint32_t 	ol_i_id;
    uint32_t 	ol_supply_w_id;
    uint32_t 	ol_quantity;
    long 		ol_delivery_d;
    float 		ol_amount;
    char 		ol_dist_info[25];
} OrderLine;

typedef struct {
    int 		s_i_id; // PRIMARY KEY 2
    int 		s_w_id; // PRIMARY KEY 1
    int 		s_order_cnt;
    int 		s_remote_cnt;
    int 		s_quantity;
    float 		s_ytd;
    char 		s_data[51];
    char		s_dist_01[25];
    char 		s_dist_02[25];
    char		s_dist_03[25];
    char 		s_dist_04[25];    
    char 		s_dist_05[25];
    char 		s_dist_06[25];
    char 		s_dist_07[25];
    char 		s_dist_08[25];
    char  		s_dist_09[25];
    char 		s_dist_10[25];
} Stock __attribute__((aligned(CACHE_LINE)));

typedef struct {
    int 		w_id; // PRIMARY KEY
    float 		w_ytd;
    float 		w_tax;
    char 		w_name[11];
    char 		w_street_1[21];
    char 		w_street_2[21];
    char 		w_city[21];
    char 		w_state[4];
    char 		w_zip[10];
    //    District 	*w_district_table;
    //    Stock 		*w_stock_table;
} Warehouse;
  
typedef struct {
    uint64_t customer_id;
    uint64_t order_id;
} OrderLineIndex;

// Now phase tables
extern HashTable<uint64_t, Warehouse> 				*s_warehouse_tbl;
extern HashTable<uint64_t, District> 				*s_district_tbl;
extern HashTable<uint64_t, Customer> 				*s_customer_tbl;
extern HashTable<uint64_t, Item> 					*s_item_tbl;

extern HashTable<uint64_t, Oorder*>					*s_oorder_index;
extern HashTable<uint64_t, Stock> 					*s_stock_tbl;
extern StringTable<Customer*>						*s_last_name_index;
extern HashTable<uint64_t, uint32_t>				*s_next_delivery_tbl;

// Later phase tables
extern ConcurrentHashTable<uint64_t, Oorder>			 		*s_oorder_tbl;
extern ConcurrentHashTable<uint64_t, History> 				*s_history_tbl;
extern ConcurrentHashTable<uint64_t, NewOrder> 				*s_new_order_tbl;
extern ConcurrentHashTable<uint64_t, OrderLine> 				*s_order_line_tbl;

// Experiment parameters
extern uint32_t 									s_num_items;  
extern uint32_t 									s_num_warehouses;
extern uint32_t 									s_districts_per_wh;
extern uint32_t 									s_customers_per_dist;
    
class TPCCUtil;

class TPCCInit {
private:
    uint32_t m_num_warehouses;
    uint32_t m_dist_per_wh;
    uint32_t m_cust_per_dist;
    uint32_t m_item_count;
        
    static const uint32_t s_first_unprocessed_o_id = 2101;

    // Each of the functions below initializes an apriori allocated single 
    // row.
    void init_warehouses( TPCCUtil &random);
    void init_districts(TPCCUtil &random);
    void init_customers(TPCCUtil &random);
    void init_history(TPCCUtil &random);
    void init_orders(TPCCUtil &random);
    void init_items(TPCCUtil &random);
    void init_stock(TPCCUtil &random);
    
    // Makes sure that everything is in order.
    void test_init();

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

    // Fields 
    uint32_t m_all_local;
    int m_order_id;
    uint32_t m_customer_id;
    uint32_t m_district_id;
    uint32_t m_warehouse_id;
    float m_district_tax;
    float m_warehouse_tax;
    uint32_t *m_order_quantities;
    uint64_t *m_supplierWarehouse_ids;
    int m_num_items;
    long m_timestamp;

public:
    NewOrderTxn(uint64_t w_id, uint64_t d_id, uint64_t c_id, 
                uint64_t o_all_local, uint64_t numItems, uint64_t *itemIds,
                uint64_t *supplierWarehouseIDs, uint32_t *orderQuantities);

    bool NowPhase();
    void LaterPhase();
    static const uint64_t invalid_item_key = 0xFFFFFFFFFFFFFFFF;
};


class PaymentTxn : public Action {
private:
    static const int 		s_customer_index = 0;

    float 					m_h_amount;
    uint32_t 				m_time;
    char 					*m_warehouse_name;
    char 					*m_district_name;
    
    uint32_t 				m_w_id;
    uint32_t 				m_d_id;
    uint32_t 				m_c_id;
    uint32_t 				m_c_w_id;
    uint32_t 				m_c_d_id;
    char					*m_last_name;
    bool					m_by_name;

public:
    PaymentTxn(uint32_t w_id, uint32_t c_w_id, float h_amount, uint32_t d_id,
               uint32_t c_d_id, uint32_t c_id, char *c_last, bool c_by_name);
    
    bool NowPhase();
    void LaterPhase();
};

class StockLevelTxn : public Action {
private:
    static const int s_district_index = 0;
    int 			m_threshold;
    int 			m_stock_count;
    uint32_t 		m_warehouse_id;
    uint32_t 		m_district_id;

public:
    StockLevelTxn(uint32_t warehouse_id, uint32_t district_id, int threshold);
    bool NowPhase();
    void LaterPhase();
};

class OrderStatusTxn : public Action {
private:
    uint32_t 		m_warehouse_id;
    uint32_t 		m_district_id;
    uint32_t 		m_customer_id;
    bool 			m_c_by_name;
    char 			*m_c_last;
    uint64_t 		m_order_line_quantity;

public:
    OrderStatusTxn(uint32_t w_id, uint32_t d_id, uint32_t c_id, char *c_last, 
                   bool c_by_name);
    bool NowPhase();
    void LaterPhase();
};

class DeliveryTxn : public Action {
private:
    uint32_t 		m_warehouse_id;
    uint32_t 		m_district_id;
    uint32_t 		m_carrier_id;
    std::vector<int>		m_num_order_lines;
public:
    DeliveryTxn(uint32_t w_id, uint32_t d_id, uint32_t carrier_id);
    bool NowPhase();
    void LaterPhase();
};

class TPCCUtil {
private:
    static const int OL_I_ID_C = 7911; // in range [0, 8191]
    static const int C_ID_C = 259; // in range [0, 1023]
	// NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
	// be within [65, 119]
    static const int C_LAST_LOAD_C = 157; // in range [0, 255]
    static const int C_LAST_RUN_C = 223; // in range [0, 255]

    uint32_t m_seed;

    static void
    gen_last_name(int num, char *buf) {
        static const char *name_tokens[] = {"BAR", "OUGHT", "ABLE", "PRI", 
                                            "PRES", "ESE", "ANTI", "CALLY", 
                                            "ATION", "EING" };
        static const int token_lengths[] = {3, 5, 4, 3, 4, 3, 5, 5, 4};

        int indices[] = { num/100, (num/10)%10, num%10 };
        int offset = 0;

        for (uint32_t i = 0; i < sizeof(indices)/sizeof(*indices); ++i) {
            memcpy(buf+offset, name_tokens[indices[i]], 
                   token_lengths[indices[i]]);
            offset += token_lengths[indices[i]];
        }
        buf[offset] = '\0';
    }

    int
    gen_non_uniform_rand(int A, int C, int min, int max) {
        int range = max - min + 1;
        int diff = 
            ((gen_rand_range(0, A) | gen_rand_range(min, max)) + C) % range;
        return diff;
    }
    
public:
    TPCCUtil() {
        m_seed = time(NULL);
    }

    int
    gen_customer_id() {
        int ret = gen_non_uniform_rand(1023, C_ID_C, 0, s_customers_per_dist-1);
        assert(ret >= 0 && ret < (int)s_customers_per_dist);
        return ret;
    }    

    void
    gen_last_name_load(char *buf) {
        gen_last_name(gen_non_uniform_rand(255, C_LAST_LOAD_C, 0, 999), buf);
    }

    void
    gen_last_name_run(char *buf) {
        gen_last_name(gen_non_uniform_rand(255, C_LAST_RUN_C, 0, 999), buf);
    }

    int
    gen_item_id() {
        return gen_non_uniform_rand(8191, OL_I_ID_C, 1, 100000);
    }
        
    int
    gen_rand_range(int min, int max) {
        int range = max - min + 1;
        return min + (rand_r(&m_seed) % range);
    }    

    void
    gen_rand_string(int min, int max, char *buf) {
        int ch_first = (int)'a', ch_last = (int)'z';
        int length = gen_rand_range(min, max);
        for (int i = 0; i < length; ++i) {
            buf[i] = (char)gen_rand_range(ch_first, ch_last);
        }
        buf[length] = '\0';        
    }
    
    static void
    append_strings(char *dest, const char **sources, int dest_len, 
                   int num_sources) {
        int offset = 0;
        for (int i = 0; i < num_sources; ++i) {
            strcpy(dest+offset, sources[i]);
            offset += strlen(sources[i]);
        }
        dest[offset] = '\0';
    }
};


#endif // TPCC_H_
