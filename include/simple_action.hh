#ifndef 		SIMPLE_ACTION_HH_
#define 		SIMPLE_ACTION_HH_

#include <action.h>
#include <one_dim_table.hh>
#include <random>

#define 	SIMPLE_RECORD_SIZE		256

namespace simple {
    struct SimpleRecord {
        uint32_t 	value[SIMPLE_RECORD_SIZE/4];

        SimpleRecord() {
            for (size_t i = 0; i < SIMPLE_RECORD_SIZE/4; ++i) {
                value[i] = (uint32_t)rand();
            }
        }

        void
        UpdateRecord() {
            for (size_t i = 0; i < SIMPLE_RECORD_SIZE/4; ++i) {
                value[i] += 1;
            }
        }
    };

    static OneDimTable<SimpleRecord> 			*s_simple_table;

    static void
    do_init(uint32_t num_records) {
        srand(time(NULL));
        s_simple_table = new OneDimTable<SimpleRecord>(num_records);
    }

    class SimpleAction : public Action {
    public:
        virtual bool
        NowPhase() {
            return true;
        }
    
        virtual void
        LaterPhase() {
            for (size_t i = 0; i < writeset.size(); ++i) {
                SimpleRecord *record = 
                    s_simple_table->GetPtr(writeset[i].record.m_key);
                record->UpdateRecord();
            }
        }
    };

    class SimpleEagerAction : public EagerAction {
    public:
        virtual void
        Execute() {
            for (size_t i = 0; i < writeset.size(); ++i) {
                SimpleRecord *record = 
                    s_simple_table->GetPtr(writeset[i].record.m_key);
                record->UpdateRecord();
            }
        }
    };
};
#endif		//  SIMPLE_ACTION_HH_
