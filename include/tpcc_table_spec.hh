#ifndef 		TPCC_TABLE_SPEC_HH_
#define 		TPCC_TABLE_SPEC_HH_

#include <tpcc.hh>
#include <concurrency_control_params.hh>

using namespace tpcc;
using namespace cc_params;

static void
GetEmptyTableInit(TableInit *scratch) {
    scratch->m_table_type = NONE;
}

static void
GetWarehouseTableInit(TableInit *scratch) {
    scratch->m_table_type = ONE_DIM_TABLE;
    scratch->m_params.m_one_params.m_dim1 = s_num_warehouses;
}

static void
GetDistrictTableInit(TableInit *scratch) {
    scratch->m_table_type = TWO_DIM_TABLE;
    scratch->m_params.m_two_params.m_dim1 = s_num_warehouses;
    scratch->m_params.m_two_params.m_dim2 = s_districts_per_wh;
    scratch->m_params.m_two_params.m_access1 = TPCCKeyGen::get_warehouse_key;
    scratch->m_params.m_two_params.m_access2 = TPCCKeyGen::get_district_key;
}

static void
GetCustomerTableInit(TableInit *scratch) {
    scratch->m_table_type = THREE_DIM_TABLE;
    scratch->m_params.m_three_params.m_dim1 = s_num_warehouses;
    scratch->m_params.m_three_params.m_dim2 = s_districts_per_wh;
    scratch->m_params.m_three_params.m_dim3 = s_customers_per_dist;
    scratch->m_params.m_three_params.m_access1 = TPCCKeyGen::get_warehouse_key;
    scratch->m_params.m_three_params.m_access2 = TPCCKeyGen::get_district_key;
    scratch->m_params.m_three_params.m_access3 = TPCCKeyGen::get_customer_key;
}

static void
GetHistoryTableInit(TableInit *scratch) {
    scratch->m_table_type = NONE;
}

static void
GetNewOrderTableInit(uint32_t size, TableInit *scratch) {
    scratch->m_table_type = CONCURRENT_HASH_TABLE;
    scratch->m_params.m_conc_params.m_size = size;
    scratch->m_params.m_conc_params.m_chain_bound = 20;
}

static void
GetOpenOrderTableInit(uint32_t size, TableInit *scratch) {
    scratch->m_table_type = CONCURRENT_HASH_TABLE;
    scratch->m_params.m_conc_params.m_size = size;
    scratch->m_params.m_conc_params.m_chain_bound = 20;
}

static void
GetOrderLineTableInit(TableInit *scratch) {
    scratch->m_table_type = NONE;
}

static void
GetItemTableInit(TableInit *scratch) {
    scratch->m_table_type = NONE;
}

static void
GetStockTableInit(TableInit *scratch) {
    scratch->m_table_type = TWO_DIM_TABLE;
    scratch->m_params.m_two_params.m_dim1 = s_num_warehouses;
    scratch->m_params.m_two_params.m_dim2 = s_num_items;
    scratch->m_params.m_two_params.m_access1 = TPCCKeyGen::get_warehouse_key;
    scratch->m_params.m_two_params.m_access2 = TPCCKeyGen::get_stock_key;
}

static void
GetOpenOrderIndexTableInit(TableInit *scratch) {
    GetCustomerTableInit(scratch);
}

// Essentially the same as the customer table
static void
GetNextDeliveryTableInit(TableInit *scratch) {
    GetDistrictTableInit(scratch);
}

#endif 		 // TPCC_TABLE_SPEC_HH_
