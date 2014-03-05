#ifndef 		LAZY_TPCC_HH_
#define 		LAZY_TPCC_HH_

#include <tpcc.hh>

using namespace tpcc;

class NewOrderTxn : public Action {
    
private:

    // Read set indices
    static const uint32_t 	s_customer_index = 0;
    static const uint32_t 	s_item_index = 1;

    // Write set indices
    static const uint32_t 	s_stock_index = 0;

    // Fields 
    uint32_t 				m_all_local;
    int 					m_order_id;
    uint32_t 				m_customer_id;
    uint32_t 				m_district_id;
    uint32_t 				m_warehouse_id;
    float 					m_district_tax;
    float 					m_warehouse_tax;
    uint32_t 				*m_order_quantities;
    uint64_t 				*m_supplierWarehouse_ids;
    uint64_t 				*m_item_ids;
    int 					m_num_items;
    long 					m_timestamp;

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

class StockLevelTxn1;
class StockLevelTxn0 : public Action {
private:
    StockLevelTxn1 *m_level1_txn;

protected:
    int 			m_threshold;
    int 			m_num_stocks;
    uint32_t 		m_warehouse_id;
    uint32_t 		m_district_id;
    uint32_t 		m_next_order_id;    

public:
    StockLevelTxn0(uint32_t warehouse_id, uint32_t district_id, int threshold, 
                   StockLevelTxn1 *level1_txn);
    virtual bool NowPhase();
    virtual void LaterPhase();
    virtual bool IsLinked(Action **action);
};

class StockLevelTxn2;
class StockLevelTxn1 : public StockLevelTxn0 {
private:
    StockLevelTxn2 *m_level2_txn;

public:
    StockLevelTxn1(uint32_t warehouse_id, uint32_t district_id, int threshold, 
                   StockLevelTxn2 *level2_txn);
    virtual bool NowPhase();
    virtual void LaterPhase();
    virtual bool IsLinked(Action **action);
};

class StockLevelTxn3;
class StockLevelTxn2 : public StockLevelTxn1 {
public:
    StockLevelTxn2(uint32_t warehouse_id, uint32_t district_id, int threshold);

    virtual bool NowPhase();
    virtual void LaterPhase();
};

class OrderStatusTxn1;
class OrderStatusTxn0 : public Action {
private:
    OrderStatusTxn1 	*m_level1_txn;
protected:
    uint32_t 			m_warehouse_id;
    uint32_t 			m_district_id;
    uint32_t 			m_customer_id;
    bool 				m_c_by_name;
    char 				*m_c_last;
    uint64_t 			m_order_line_quantity;

public:
    OrderStatusTxn0(uint32_t w_id, uint32_t d_id, uint32_t c_id, char *c_last, 
                    bool c_by_name, OrderStatusTxn1 *level1_txn, bool do_init);
    bool NowPhase();
    void LaterPhase();
    virtual bool IsLinked(Action **action);
};

class OrderStatusTxn1 : public OrderStatusTxn0 {
public:
    OrderStatusTxn1(uint32_t w_id, uint32_t d_id, uint32_t c_id, char *c_last, 
                    bool c_by_name);
    bool NowPhase();
    void LaterPhase();
    virtual bool IsLinked(Action **action);
};

class LazyCustAmt {
public:
    uint64_t 		m_customer_key;
    uint32_t 		m_amount;    
};

class DeliveryTxn1;
class DeliveryTxn0 : public Action {
private:
    DeliveryTxn1 			*m_level1_txn;

protected:
    uint32_t 				m_warehouse_id;
    uint32_t 				m_district_id;
    uint32_t 				m_carrier_id;
    uint32_t				*m_num_order_lines;
    uint32_t 				*m_open_order_ids;
    LazyCustAmt				*m_amounts;
    
public:
    DeliveryTxn0(uint32_t w_id, uint32_t d_id, uint32_t carrier_id, 
                DeliveryTxn1 *level1_txn);
    virtual bool NowPhase();
    virtual void LaterPhase();
    virtual bool IsLinked(Action **action);
};

class DeliveryTxn2;
class DeliveryTxn1 : public DeliveryTxn0 {
private:
    DeliveryTxn2 			*m_level2_txn;
public:
    DeliveryTxn1(uint32_t w_id, uint32_t d_id, uint32_t carrier_id, 
                 DeliveryTxn2 *level2_txn);

    virtual bool NowPhase();
    virtual void LaterPhase();
    virtual bool IsLinked(Action **action);
};

class DeliveryTxn2 : public DeliveryTxn1 {
public:
    DeliveryTxn2(uint32_t w_id, uint32_t d_id, uint32_t carrier_id);
    virtual bool NowPhase();
    virtual void LaterPhase();
    virtual bool IsLinked(Action **action);
};

#endif		//  LAZY_TPCC_HH_
