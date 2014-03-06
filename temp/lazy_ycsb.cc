#include <lazy_ycsb.hh>
#include 

using namespace lazy_ycsb;
using namespace ycsb;

void
ycsb::DoInit(uint64_t num_records) {
    s_num_records = num_records;
    s_ycscb_table = new OneDimTable<YCSBRecord>(num_records);
    char buf[YCSB_RECORD_SIZE];
    for (uint64_t i = 0; i < num_records; ++i) {
        InitSingle((uint64_t*)buf, YCSB_RECORD_SIZE/sizeof(uint64_t));
        YCSBRecord *record = s_ycsb_table->GetPtr(i);
        memcpy(record->value, buf, YCSB_RECORD_SIZE);
    }
}

void
ycsb::InitSingle(uint64_t *values, uint32_t num_values) {
    for (uint32_t i = 0; i < num_values; ++i) {
        values[i] = (uint32_t)rand();
        values[i] = (values[i] << 32) | ((uint32_t)rand());
    }
}

lazy_ycsb::YCSBTxn::YCSBTxn(uint64_t key) {
    m_key = key;
}

bool
lazy_ycsb::YCSBTxn::NowPhase() {
    return true;
}

lazy_ycsb::YCSBRead::YCSBRead(uint64_t key) 
    : YCSBTxn(key) {
    struct DependencyInfo dep_info;
    dep_info.dependency = NULL;
    dep_info.is_write = false;
    dep_info.index = -1;
    
    dep_info.record.m_table = YCSB_TABLE;
    dep_info.record.m_key = key;
    readset.push_back(dep_info);
}


void
lazy_ycsb::YCSBRead::LaterPhase() {
    YCSBRecord *record = s_ycsb_table->GetPtr(m_key);
    memcpy(m_read_values, record->value, YCSB_RECORD_SIZE);
}

void
lazy_ycsb::YCSBRMW::LaterPhase() {
    YCSBRecord *record = s_ycsb_table->GetPtr(m_key);
    for (size_t i = 2; i < YCSB_RECORD_SIZE/sizeof(uint64_t); ++i) {
        record[i] += 1;
    }
}

void
lazy_ycsb::YCSBWrite::LaterPhase() {
    YCSBRecord *record = s_ycsb_table->GetPtr(m_key);
    memcpy(record->value, m_write_values, YCSB_RECORD_SIZE);
}
