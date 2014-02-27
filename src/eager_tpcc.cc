#include <eager_tpcc.hh>
#include <algorithm>
#include <iostream>

NewOrderEager::NewOrderEager(uint64_t w_id, uint64_t d_id, uint64_t c_id, 
                             uint64_t o_all_local, uint64_t num_items, 
                             uint64_t *item_ids, 
                             uint64_t *supplier_wh_ids, 
                             uint32_t *order_quantities) {
    uint32_t keys[4];
    struct EagerRecordInfo info;    

    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_customer_id = c_id;
    m_num_items = num_items;
    m_item_ids = item_ids;
    m_order_quantities = order_quantities;
    m_supplier_wh_ids = supplier_wh_ids;
    m_all_local = o_all_local;
    
    // Create the warehouse key
    info.record.m_table = WAREHOUSE;
    info.record.m_key = w_id;
    readset.push_back(info);
    
    // Create the district key
    keys[0] = w_id;
    keys[1] = d_id;
    info.record.m_table = DISTRICT;
    info.record.m_key = TPCCKeyGen::create_district_key(keys);
    writeset.push_back(info);

    // Create the customer key
    keys[2] = c_id;
    info.record.m_table = CUSTOMER;
    info.record.m_key = TPCCKeyGen::create_customer_key(keys);
    readset.push_back(info);

    // Create a key for customer key indexed open order
    info.record.m_table = OPEN_ORDER_INDEX;
    writeset.push_back(info);
    
    // Create the stock keys
    info.record.m_table = STOCK;
    for (size_t i = 0; i < num_items; ++i) {
        keys[1] = item_ids[i];
        info.record.m_key = TPCCKeyGen::create_stock_key(keys);
        writeset.push_back(info); 
    }

    // Make sure that all txns request locks in the same sorted order to avoid 
    // deadlocks
    std::sort(readset.begin(), readset.end());
    std::sort(writeset.begin(), writeset.end());
}

bool
NewOrderEager::IsRoot() {
    return true;
}

bool
NewOrderEager::IsLinked(EagerAction **ret) {
    *ret = NULL;
    return false;
}

void
NewOrderEager::PostExec() {}

void
NewOrderEager::Execute() {
    uint32_t keys[4];    
    
    // A NewOrder txn aborts if any of the item keys are invalid. 
    for (uint32_t i = 0; i < m_num_items; ++i) {

        // Make sure that all the items requested exist. 
        if (m_item_ids[i] == invalid_item_key) {
            return;
        }
    }    

    // Update the district record
    assert(writeset[s_district_index].record.m_table == DISTRICT);
    uint64_t d_key = writeset[s_district_index].record.m_key;
    District *district = s_district_tbl->GetPtr(d_key);
    uint32_t order_id = district->d_next_o_id;
    float district_tax = district->d_tax;
    district->d_next_o_id += 1;
        
    float warehouse_tax = s_warehouse_tbl->GetPtr(m_warehouse_id)->w_tax;
    
    // XXX: Hope this does not serialize in the OS!
    // m_timestamp = time(NULL);
    uint32_t timestamp = 0;
    
    float total_amount = 0;
    CompositeKey composite;
    
    // Read the customer record.
    assert(readset[s_customer_index].record.m_table == CUSTOMER);
    Customer *customer = 
        s_customer_tbl->GetPtr(readset[s_customer_index].record.m_key);
    float c_discount = customer->c_discount;
    
    
    //
    // BEGIN: INSERT NEWORDER RECORD
    //
    // Create the NewOrder record
    NewOrder new_order_record;
    new_order_record.no_w_id = m_warehouse_id;
    new_order_record.no_d_id = m_district_id;
    new_order_record.no_o_id = order_id;
    
    // Create the NewOrder key
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;
    keys[2] = order_id;
    uint64_t new_order_key = TPCCKeyGen::create_new_order_key(keys);

    // Insert the NewOrder record
    s_new_order_tbl->Put(new_order_key, new_order_record);
    
    //
    // END: INSERT NEWORDER RECORD
    //
  
    for (size_t i = 0; i < m_num_items; ++i) {
        composite = writeset[s_stock_index+i].record;
        assert(composite.m_table == STOCK);
        uint64_t ol_w_id = TPCCKeyGen::get_warehouse_key(composite.m_key);
        uint64_t ol_s_id = TPCCKeyGen::get_stock_key(composite.m_key);
        uint64_t ol_i_id = m_item_ids[i];        
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
        new_order_line.ol_o_id = order_id;
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
        s_order_line_tbl->Put(writeset[m_num_items+1+i].record.m_key, 
                              new_order_line);
    }
    // Insert an entry into the open order table.    
    Oorder oorder;
    oorder.o_id = order_id;
    oorder.o_w_id = m_warehouse_id;
    oorder.o_d_id = m_district_id;
    oorder.o_c_id = m_customer_id;
    oorder.o_carrier_id = 0;
    oorder.o_ol_cnt = m_num_items;
    oorder.o_all_local = m_all_local;
    oorder.o_entry_d = timestamp;
    Oorder *new_oorder = 
        s_oorder_tbl->Put(writeset[2*m_num_items+1].record.m_key, oorder);
    uint64_t *old_index = 
        s_oorder_index->GetPtr(readset[s_customer_index].record.m_key);
    *old_index = writeset[2*m_num_items+1].record.m_key;
    
}

PaymentEager::PaymentEager(uint32_t w_id, uint32_t c_w_id, float h_amount,
                           uint32_t d_id, uint32_t c_d_id, uint32_t c_id, 
                           char *c_last, bool c_by_name) {

    assert(w_id < s_num_warehouses);
    assert(c_w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);
    
    m_w_id = w_id;
    m_c_w_id = c_w_id;
    m_h_amount = h_amount;
    m_d_id = d_id;
    m_c_d_id = c_d_id;
    m_c_id = c_id;
    m_c_last = c_last;
    m_c_by_name = c_by_name;

    struct EagerRecordInfo info;
    uint32_t keys[4];
    
    // Add the warehouse to the writeset
    info.record.m_table = WAREHOUSE;
    info.record.m_key = w_id;
    writeset.push_back(info);    
    
    // Add the district to the writeset
    keys[0] = w_id;
    keys[1] = d_id;
    info.record.m_key = TPCCKeyGen::create_district_key(keys);
    info.record.m_table = DISTRICT;
    writeset.push_back(info);

    // Add the customer to the writeset
    keys[0] = c_w_id;
    keys[1] = c_d_id;
    keys[2] = c_id;
    info.record.m_key = TPCCKeyGen::create_customer_key(keys);
    info.record.m_table = CUSTOMER;
    writeset.push_back(info);
    
    // Sort the elements in the writeset, make sure that the readset is empty
    std::sort(writeset.begin(), writeset.end());
    assert(readset.size() == 0);

    // Make sure that all the keys are placed in their expected locations
    assert(writeset[s_warehouse_index].record.m_table == WAREHOUSE);
    assert(writeset[s_district_index].record.m_table == DISTRICT);
    assert(writeset[s_customer_index].record.m_table == CUSTOMER);
}

bool
PaymentEager::IsRoot() {
    return true;
}

bool
PaymentEager::IsLinked(EagerAction **ret) {
    *ret = NULL;
    return false;
}

void
PaymentEager::PostExec() { }

void
PaymentEager::Execute() {

    // Update the warehouse
    assert(writeset[s_warehouse_index].record.m_table == WAREHOUSE);
    Warehouse *warehouse = s_warehouse_tbl->GetPtr(writeset[s_warehouse_index].record.m_key);
    warehouse->w_ytd += m_h_amount;

    // Update the district
    assert(writeset[s_district_index].record.m_table == DISTRICT);
    District *district = s_district_tbl->GetPtr(writeset[s_district_index].record.m_key);
    district->d_ytd += m_h_amount;

    char *warehouse_name = warehouse->w_name;
    char *district_name = district->d_name;

    // Update the customer
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
    const char *holder[3] = {warehouse_name, empty, district_name};
    TPCCUtil::append_strings(hist.h_data, holder, 26, 3);
    s_history_tbl->Put(writeset[s_customer_index].record.m_key, hist);    
}

StockLevelEager0::StockLevelEager0(uint32_t warehouse_id, uint32_t district_id, 
                                   int threshold, 
                                   StockLevelEager1 *level1_txn, bool do_init) {
    assert(warehouse_id < s_num_warehouses);
    assert(district_id < s_districts_per_wh);
    
    m_threshold = threshold;
    m_level1_txn = level1_txn;
    m_next_order_id = 0;
    m_num_stocks = 0;
    
    if (do_init) {
        uint32_t keys[2];
        keys[0] = m_warehouse_id;
        keys[1] = m_district_id;
        struct EagerRecordInfo info;
        info.record.m_table = DISTRICT;
        info.record.m_key = TPCCKeyGen::create_district_key(keys);
        readset.push_back(info);
        uint32_t temp = TPCCKeyGen::get_district_key(readset[0].record.m_key);
        if (temp != m_district_id) {
            std::cout << "blah!\n";
        }
        assert(temp == m_district_id);
    }
}

bool
StockLevelEager0::IsRoot() {
    return true;
}

bool
StockLevelEager0::IsLinked(EagerAction **ret) {
    *ret = m_level1_txn;
    return true;
}

void
StockLevelEager0::Execute() {
    assert(readset[0].record.m_table == DISTRICT);
    District *dist = s_district_tbl->GetPtr(readset[0].record.m_key);    
    m_next_order_id = dist->d_next_o_id;
}

void
StockLevelEager0::PostExec() {
    assert(m_next_order_id != 0);
    struct EagerRecordInfo dep_info;

    uint32_t keys[3];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;

    dep_info.record.m_table = OPEN_ORDER;
    for (uint32_t i = 0; i < 20; ++i) {
        keys[2] = m_next_order_id - 1 -i;
        uint64_t open_order_key = TPCCKeyGen::create_order_key(keys);
        dep_info.record.m_key = open_order_key;
        m_level1_txn->readset.push_back(dep_info);
    }
    std::sort(m_level1_txn->readset.begin(), m_level1_txn->readset.end());
}

StockLevelEager1::StockLevelEager1(uint32_t warehouse_id, uint32_t district_id, 
                                   int threshold, 
                                   StockLevelEager2 *level2_txn) 
    : StockLevelEager0(warehouse_id, district_id, threshold, this, false) {
    m_level2_txn = level2_txn;
}

bool
StockLevelEager1::IsLinked(EagerAction **ret) {
    *ret = m_level2_txn;
    return true;
}

// Get the primary keys of the stock
void
StockLevelEager1::Execute() {
    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;

    for (size_t i = 0; i < readset.size(); ++i) {
        assert(readset[i].record.m_table == OPEN_ORDER);
        Oorder *oorder = s_oorder_tbl->GetPtr(readset[i].record.m_key);
        keys[2] = oorder->o_id;
        for (uint32_t j = 0; j < oorder->o_ol_cnt; ++j) {
            keys[3] = j;
            uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
            OrderLine *order_line = s_order_line_tbl->GetPtr(order_line_key);
            m_stock_ids.push_back(order_line->ol_i_id);
        }
    }
}


// Insert each of the stocks into the readset
void
StockLevelEager1::PostExec() {
    
    // Stuff to create the keys
    struct EagerRecordInfo info;
    info.record.m_table = STOCK;    
    uint32_t keys[2];
    keys[0] = m_warehouse_id;

    // Create the keys and insert them into the readset
    uint32_t num_stocks = m_stock_ids.size();
    for (uint32_t i = 0; i < num_stocks; ++i) {
        assert(m_stock_ids[i] < s_num_items);
        keys[1] = m_stock_ids[i];
        info.record.m_key = TPCCKeyGen::create_stock_key(keys);
        m_level2_txn->readset.push_back(info);
    }
    std::sort(m_level2_txn->readset.begin(), m_level2_txn->readset.end());
}


StockLevelEager2::StockLevelEager2(uint32_t warehouse_id, uint32_t district_id, 
                                   int threshold) 
    : StockLevelEager1(warehouse_id, district_id, threshold, this) {
    m_num_stocks = 0;
}

bool
StockLevelEager2::IsLinked(EagerAction **ret) {
    *ret = NULL;
    return false;
}

void
StockLevelEager2::Execute() {
    m_num_stocks = 0;
    uint32_t num_stocks = readset.size();
    for (uint32_t i = 0; i < num_stocks; ++i) {
        assert(readset[i].record.m_table == STOCK);
        Stock *stock = s_stock_tbl->GetPtr(readset[i].record.m_key);
        m_num_stocks += ((uint32_t)(stock->s_quantity - m_threshold)) >> 31;
    }
}

void
StockLevelEager2::PostExec() {

}

OrderStatusEager0::OrderStatusEager0(uint32_t w_id, uint32_t d_id, 
                                     uint32_t c_id, char *c_last, 
                                     bool c_by_name, 
                                     OrderStatusEager1 *level1_txn, 
                                     bool do_init) {
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);
    assert(c_id < s_customers_per_dist);
    
    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_customer_id = c_id;
    m_c_by_name = c_by_name;
    m_c_last = c_last;
    m_level1_txn = level1_txn;

    if (do_init) {
        struct EagerRecordInfo info;
        info.record.m_table = OPEN_ORDER_INDEX;
        uint32_t keys[3];
        keys[0] = m_warehouse_id;
        keys[1] = m_district_id;
        keys[2] = m_customer_id;
        info.record.m_key = TPCCKeyGen::create_customer_key(keys);
        readset.push_back(info);
    }
}

bool
OrderStatusEager0::IsRoot() {
    return true;
}

bool
OrderStatusEager0::IsLinked(EagerAction **ret) {
    *ret = m_level1_txn;
    return true;
}

void
OrderStatusEager0::Execute() {
    assert(readset[0].record.m_table == OPEN_ORDER_INDEX);
    m_open_order_key = s_oorder_index->Get(readset[0].record.m_key);
}

void
OrderStatusEager0::PostExec() {
    struct EagerRecordInfo info;
    info.record.m_table = OPEN_ORDER;
    info.record.m_key = m_open_order_key;
    m_level1_txn->readset.push_back(info);
}


OrderStatusEager1::OrderStatusEager1(uint32_t w_id, uint32_t d_id, 
                                     uint32_t c_id, char *c_last, 
                                     bool c_by_name)
    : OrderStatusEager0(w_id, d_id, c_id, c_last, c_by_name, this, false) {

}

bool
OrderStatusEager1::IsLinked(EagerAction **ret) {
    *ret = NULL;
    return false;
}

void
OrderStatusEager1::Execute() {
    uint32_t keys[4];
    keys[0] = m_warehouse_id;
    keys[1] = m_district_id;
    
    Oorder *oorder = s_oorder_tbl->GetPtr(readset[0].record.m_key);
    keys[2] = oorder->o_id;
    for (uint32_t i = 0; i < oorder->o_ol_cnt; ++i) {
        keys[3] = i;        
        uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
        OrderLine *ol = s_order_line_tbl->GetPtr(order_line_key);
        m_order_line_quantity += ol->ol_quantity;
    }

}

void
OrderStatusEager1::PostExec() {

}

DeliveryEager0::DeliveryEager0(uint32_t w_id, uint32_t d_id, 
                               uint32_t carrier_id, 
                               DeliveryEager1 *level1_txn, bool do_init) {
    assert(w_id < s_num_warehouses);
    assert(d_id < s_districts_per_wh);

    m_warehouse_id = w_id;
    m_district_id = d_id;
    m_carrier_id = carrier_id;    
    m_level1_txn = level1_txn;

    m_amounts = (CustomerAmt*)malloc(sizeof(CustomerAmt)*s_districts_per_wh);
    m_open_order_ids = (uint32_t*)malloc(sizeof(uint32_t)*s_districts_per_wh);
    m_num_order_lines = (uint32_t*)malloc(sizeof(uint32_t)*s_districts_per_wh);
    m_customer_keys = (uint64_t*)malloc(sizeof(uint64_t)*s_districts_per_wh);

    memset(m_num_order_lines, 0, sizeof(uint32_t)*s_districts_per_wh);
    memset(m_open_order_ids, 0, sizeof(uint32_t)*s_districts_per_wh);
    memset(m_amounts, 0xFF, sizeof(CustomerAmt)*s_districts_per_wh);
    memset(m_customer_keys, 0, sizeof(uint64_t)*s_districts_per_wh);

    if (do_init) {
        struct EagerRecordInfo info;
        uint32_t keys[2];
        keys[0] = m_warehouse_id;
        for (uint32_t i = 0; i < s_districts_per_wh; ++i) {
            keys[1] = i;
            info.record.m_table = DISTRICT;
            info.record.m_key = TPCCKeyGen::create_district_key(keys);
            readset.push_back(info);
        
            info.record.m_table = NEXT_DELIVERY;
            writeset.push_back(info);
        }
        std::sort(readset.begin(), readset.end());
        std::sort(writeset.begin(), writeset.end());    
    }
    assert(readset.size() == writeset.size());    
    
}

bool
DeliveryEager0::IsRoot() {
    return true;
}

bool
DeliveryEager0::IsLinked(EagerAction **ret) {
    *ret = m_level1_txn;
    return true;
}

void
DeliveryEager0::Execute() {
    uint32_t num_reads = readset.size();
    for (uint32_t i = 0; i < num_reads; ++i) {
        assert(readset[i].record.m_table == DISTRICT);
        assert(writeset[i].record.m_table == NEXT_DELIVERY);

        District *district = s_district_tbl->GetPtr(readset[i].record.m_key);
        uint32_t *order_id = 
            s_next_delivery_tbl->GetPtr(writeset[i].record.m_key);        
        if (district->d_next_o_id > *order_id) {
            m_open_order_ids[i] = *order_id;
            *order_id += 1;
        }
    }
}

void
DeliveryEager0::PostExec() {
    uint32_t keys[3];
    keys[0] = m_warehouse_id;

    struct EagerRecordInfo info;
    info.record.m_table = OPEN_ORDER;

    for (uint32_t i = 0; i < s_districts_per_wh; ++i) {
        keys[1] = i;
        if (m_open_order_ids[i] > 0) {
            keys[2] = m_open_order_ids[i];
            info.record.m_key = TPCCKeyGen::create_order_key(keys);
            m_level1_txn->writeset.push_back(info);
        }
    }
    std::sort(m_level1_txn->writeset.begin(), m_level1_txn->writeset.end());
}

DeliveryEager1::DeliveryEager1(uint32_t w_id, uint32_t d_id, 
                               uint32_t carrier_id, 
                               DeliveryEager2 *level2_txn) 
    : DeliveryEager0(w_id, d_id, carrier_id, this, false) {
    m_level2_txn = level2_txn;
}

void
DeliveryEager1::Execute() {
    uint32_t keys[4];

    uint32_t writeset_size = writeset.size();
    for (uint32_t i = 0; i < writeset_size; ++i) {
        assert(writeset[i].record.m_table == OPEN_ORDER);
        Oorder *oorder = s_oorder_tbl->GetPtr(writeset[i].record.m_key);
        uint32_t num_items = oorder->o_ol_cnt;
        uint32_t amount = 0;
        
        keys[0] = oorder->o_w_id;
        keys[1] = oorder->o_d_id;
        keys[2] = oorder->o_c_id;        
        m_customer_keys[i] = TPCCKeyGen::create_customer_key(keys);
        m_amounts[i].m_customer_key = m_customer_keys[i];

        keys[2] = oorder->o_id;
        m_amounts[i].m_amount = 0;
        for (uint32_t j = 0; j < num_items; ++j) {
            keys[3] = j;
            uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
            OrderLine *order_line = s_order_line_tbl->GetPtr(order_line_key);
            m_amounts[i].m_amount += order_line->ol_amount;
        }
    }
}

int
DeliveryEager1::GetIndex(const std::vector<struct EagerRecordInfo> &info, uint32_t size, 
                         const struct EagerRecordInfo &cmp) {
    for (uint32_t j = 0; j < size; ++j) {
        assert(info[j].record.m_table == cmp.record.m_table);
        if (info[j].record.m_key == cmp.record.m_key) {
            assert((int)j == j);
            return (int)j;
        }
    }
    return -1;
}

void
DeliveryEager1::PostExec() {
    struct EagerRecordInfo info;
    info.record.m_table = CUSTOMER;

    for (uint32_t i = 0; i < writeset.size(); ++i) {
        info.record.m_key = m_customer_keys[i];
        uint32_t j = 0;
        for (j = 0; j < i; ++j) {
            if (m_level2_txn->writeset[j].record.m_key == info.record.m_key) {
                break;
            }
        }

        if (j < i) {	// We've found a duplicate
            assert(m_level2_txn->m_amounts[j].m_customer_key == info.record.m_key);
            m_level2_txn->m_amounts[j].m_amount += m_level2_txn->m_amounts[i].m_amount;
        }
        else {
            assert(m_amounts[i].m_customer_key == info.record.m_key);
            m_level2_txn->writeset.push_back(info);
            size_t index = m_level2_txn->writeset.size()-1;
            m_level2_txn->m_amounts[index] = m_amounts[i];
        }
    }

    std::sort(m_level2_txn->writeset.begin(), m_level2_txn->writeset.end());
    std::sort(&m_level2_txn->m_amounts[0], &m_level2_txn->m_amounts[s_districts_per_wh]);
}

bool
DeliveryEager1::IsLinked(EagerAction **ret) {
    *ret = m_level2_txn;
    return true;
}


DeliveryEager2::DeliveryEager2(uint32_t w_id, uint32_t d_id, 
                               uint32_t carrier_id) 
    : DeliveryEager1(w_id, d_id, carrier_id, this) {

}

bool
DeliveryEager2::IsLinked(EagerAction **ret) {
    ret = NULL;
    return false;
}

void
DeliveryEager2::Execute() {
    uint32_t num_customers = writeset.size();
    for (uint32_t i = 0; i < num_customers; ++i) {
        assert(writeset[i].record.m_key == m_amounts[i].m_customer_key);
        Customer *customer = s_customer_tbl->GetPtr(writeset[i].record.m_key);
        customer->c_balance += m_amounts[i].m_amount;
        customer->c_delivery_cnt += 1;
    }
}

void
DeliveryEager2::PostExec() { }
