#ifndef 	YCSB_HH_
#define		YCSB_HH_

#include 		<table.hh>
#include 		<action.h>
#include 		<machine.h>

#define 	YCSB_TABLE 				0
#define 	YCSB_RECORD_SIZE		1000
#define 	YCSB_RECORD_FIELDS		10
#define 	YCSB_KEY_SIZE			2
#define 	YCSB_VALUE_OFFSET		2

namespace ycsb {
    
    typedef struct {
        char 		value[1000];
    } __attribute__((aligned(CACHE_LINE))) YCSBRecord;

    static uint64_t 						s_num_records;
    static Table<uint64_t, YCSBRecord>		*s_ycsb_table;

    void
    DoInit(uint32_t num_records);
};

namespace lazy_ycsb {
    class YCSBTxn : public Action {
    protected:
        uint64_t 		m_key;
        
    public:
        YCSBTxn(uint64_t key);

        virtual bool
        NowPhase();
    };
        
    class YCSBRead : public YCSBTxn {
    private:
        char		m_read_values[YCSB_RECORD_SIZE];

    public:
        YCSBRead(uint64_t key);
        
        virtual void
        LaterPhase();      
    };
    
    class YCSBRMW : public YCSBTxn {
    public:
        YCSBRMW(uint64_t key);
        
        virtual void
        LaterPhase();
    };
      
    class YCSBWrite : public YCSBTxn {
    private:
        char 		m_write_values[YCSB_RECORD_SIZE];
        
    public:
        YCSBWrite(uint64_t key);
        
        virtual void
        LaterPhase();
    };

    class YCSBInsert : public YCSBTxn {
    private:
        char 		m_values[YCSB_RECORD_SIZE];
        
    public:
        YCSBWrite(uint64_t key);
        
        virtual bool
        LaterPhase();
    };
};

#endif		// YCSB_HH_
