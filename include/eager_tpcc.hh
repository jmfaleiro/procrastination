#ifndef 	EAGER_TPCC_HH_
#define		EAGER_TPCC_HH_

#include <tpcc.hh>
#include <action.h>

class NewOrderEager : public EagerAction {
private:
    static const uint64_t invalid_item_key = 0xFFFFFFFFFFFFFFFF;

    uint64_t 			m_warehouse_id;
    uint64_t			m_district_id;
    uint64_t 			m_customer_id;
    uint64_t 			m_num_items;
    uint64_t 			*m_item_ids;
    uint32_t 			*m_order_quantities;
    uint64_t 			*m_supplier_wh_ids;
    uint32_t 			m_all_local;

public:
    NewOrderEager(uint64_t w_id, uint64_t d_id, uint64_t c_id, 
                  uint64_t o_all_local, uint64_t num_items, 
                  uint64_t *item_ids, uint64_t *supplier_wh_ids, 
                  uint32_t *order_quantities);
    virtual bool
    IsLinked(EagerAction **ret);
    
    virtual void
    Execute();

    virtual void
    PostExec();

    // Read set indices
    static const uint32_t 			s_customer_index 	= 1;
    static const uint32_t 			s_warehouse_index   = 0;
    
    // Write set indices
    static const uint32_t 			s_district_index    = 0;
    static const uint32_t 			s_stock_index		= 1;
};

class PaymentEager : public EagerAction {
private:
    static const int 		s_warehouse_index = 0;
    static const int 		s_district_index = 1;
    static const int 		s_customer_index = 2;

    float 					m_h_amount;
    uint32_t 				m_time;
    uint32_t 				m_w_id;
    uint32_t 				m_d_id;
    uint32_t 				m_c_id;
    uint32_t 				m_c_w_id;
    uint32_t 				m_c_d_id;
    char					*m_c_last;
    bool					m_c_by_name;

public:
    PaymentEager(uint32_t w_id, uint32_t c_w_id, float h_amount, 
                 uint32_t d_id, uint32_t c_d_id, uint32_t c_id, char *c_last,
                 bool c_by_name);
    
    virtual bool
    IsLinked(EagerAction **link);
    
    virtual void
    Execute();
    
    virtual void
    PostExec();
};

class StockLevelEager1;
class StockLevelEager0 : public EagerAction {
private:
    StockLevelEager1 		*m_level1_txn;

protected:
    int 					m_threshold;
    uint32_t 				m_warehouse_id;
    uint32_t 				m_district_id;
    uint32_t 				m_next_order_id;
    uint32_t 				m_num_stocks;

public:
    StockLevelEager0(uint32_t warehouse_id, uint32_t district_id, int threshold, 
                     StockLevelEager1 *level1_txn);
    
    virtual bool IsLinked(EagerAction **ret);
    virtual void Execute();
    virtual void PostExec();
};

class StockLevelEager2;
class StockLevelEager1 : public StockLevelEager0 {
private:
    StockLevelEager2 			*m_level2_txn;
    std::vector<uint32_t>		m_stock_ids;

public:
    StockLevelEager1(uint32_t warehouse_id, uint32_t district_id, int threshold,
                     StockLevelEager2 *level2_txn);
    virtual bool IsLinked(EagerAction **ret);
    virtual void Execute();
    virtual void PostExec();
};

class StockLevelEager2 : public StockLevelEager1 {
public:
    StockLevelEager2(uint32_t warehouse_id, uint32_t district_id, 
                     int threshold);
    virtual bool IsLinked(EagerAction **ret);
    virtual void Execute();
    virtual void PostExec();
};


class OrderStatusEager1;
class OrderStatusEager0 : public EagerAction {
private:
    OrderStatusEager1			*m_level1_txn;
protected:
    uint32_t 					m_warehouse_id;
    uint32_t 					m_district_id;
    uint32_t 					m_customer_id;
    bool 						m_c_by_name;
    char						*m_c_last;
    uint64_t 					m_open_order_key;

public:
    OrderStatusEager0(uint32_t w_id, uint32_t d_id, uint32_t c_id, char *c_last,
                      bool c_by_name, OrderStatusEager1 *level1_txn);

    virtual bool
    IsLinked(EagerAction **ret);
    
    virtual void
    Execute();
    
    virtual void
    PostExec();
};

class OrderStatusEager1 : public OrderStatusEager0 {
public:
    uint32_t 		m_order_line_quantity;

    OrderStatusEager1(uint32_t w_id, uint32_t d_id, uint32_t c_id, char *c_last,
                      bool c_by_name);

    virtual bool
    IsLinked(EagerAction **ret);
    
    virtual void
    Execute();
    
    virtual void
    PostExec();
};


class DeliveryEager1;
class DeliveryEager0 : public EagerAction {

private:
    DeliveryEager1 		*m_level1_txn;

protected:
    uint32_t 			m_warehouse_id;
    uint32_t 			m_district_id;
    uint32_t 			m_carrier_id;
    uint32_t 			*m_open_order_ids;
    uint32_t 			*m_num_order_lines;
    uint32_t 			*m_amounts;
    uint64_t 			*m_customer_keys;

    
public: 
    DeliveryEager0(uint32_t w_id, uint32_t d_id, uint32_t carrier_id, 
                   DeliveryEager1 *level1_txn);
    
    virtual bool
    IsLinked(EagerAction **ret);
    
    virtual void
    Execute();
    
    virtual void
    PostExec();    
};

class DeliveryEager2;
class DeliveryEager1 : public DeliveryEager0 {
private:
    DeliveryEager2			*m_level2_txn;

public:
    DeliveryEager1(uint32_t w_id, uint32_t d_id, uint32_t carrier_id, 
                   DeliveryEager2 *level2_txn);
    
    virtual bool
    IsLinked(EagerAction **ret);
    
    virtual void
    Execute();
    
    virtual void
    PostExec();    
};

class DeliveryEager2 : public DeliveryEager1 {
public:
    DeliveryEager2(uint32_t w_id, uint32_t d_id, uint32_t carrier_id);
                   
    
    virtual bool
    IsLinked(EagerAction **ret);
    
    virtual void
    Execute();
    
    virtual void
    PostExec();    
};


#endif	//  EAGER_TPCC_HH_
