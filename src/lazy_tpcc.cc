#include <lazy_tpcc.hh>
#include <iostream>

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
        uint64_t stock_key = writeset[s_stock_index+i].record.m_key;
        assert(TPCCKeyGen::get_stock_key(stock_key) < s_num_items);
        assert(m_item_ids[i] == TPCCKeyGen::get_stock_key(stock_key));
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
    writeset[m_num_items].record.m_key = new_order_key;
    writeset[m_num_items+1].record.m_key = new_order_key;
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
        uint64_t ol_i_id = m_item_ids[i];

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
    Oorder *new_oorder = s_oorder_tbl->Put(writeset[m_num_items+1].record.m_key, oorder);
    uint64_t *old_index = s_oorder_index->GetPtr(readset[s_customer_index].record.m_key);
    *old_index = writeset[m_num_items+2].record.m_key;
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
    m_num_stocks = 0;
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
}

bool
StockLevelTxn0::IsLinked(Action **next_level) {
    *next_level = m_level1_txn;
    return true;
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
    dep_info.record.m_table = STOCK;

    uint32_t keys[4];
    keys[0] = m_warehouse_id;

    uint32_t num_reads = readset.size();
    for (uint32_t i = 0; i < num_reads; ++i) {
        keys[1] = m_district_id;
        Oorder *oorder = s_oorder_tbl->GetPtr(readset[i].record.m_key);
        keys[2] = oorder->o_id;
        for (uint32_t j = 0; j < oorder->o_ol_cnt; ++j) {
            keys[3] = j;
            uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
            OrderLine *order_line = s_order_line_tbl->GetPtr(order_line_key);
            keys[1] = order_line->ol_i_id;
            dep_info.record.m_key = TPCCKeyGen::create_stock_key(keys);
            m_level2_txn->readset.push_back(dep_info);
        }
    }
}

bool
StockLevelTxn1::IsLinked(Action **action) {
    *action = m_level2_txn;
    return true;
}

StockLevelTxn2::StockLevelTxn2(uint32_t warehouse_id, uint32_t district_id, 
                               int threshold)
    : StockLevelTxn1(warehouse_id, district_id, threshold, this) {        
}


bool
StockLevelTxn2::NowPhase() {
    return true;
}

void
StockLevelTxn2::LaterPhase() {    
    m_num_stocks = 0;
    uint32_t num_stocks = readset.size();
    for (uint32_t i = 0; i < num_stocks; ++i) {
        assert(readset[i].record.m_table == STOCK);
        Stock *stock = s_stock_tbl->GetPtr(readset[i].record.m_key);
        m_num_stocks += ((uint32_t)(stock->s_quantity - m_threshold)) >> 31;
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
    uint64_t oorder_key = s_oorder_index->Get(index);
    Oorder *oorder = s_oorder_tbl->GetPtr(oorder_key);
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

bool
OrderStatusTxn0::IsLinked(Action **action) {
    *action = m_level1_txn;
    return true;
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

bool
DeliveryTxn0::IsLinked(Action **action) {
    *action = m_level1_txn;
    return true;
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

bool
DeliveryTxn1::IsLinked(Action **action) {
    *action = m_level2_txn;
    return true;
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
