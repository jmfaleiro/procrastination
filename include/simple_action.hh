#ifndef 		SIMPLE_ACTION_HH_
#define 		SIMPLE_ACTION_HH_

#include <action.h>
#include <one_dim_table.hh>
#include <random>
#include <string.h>
#include <stdlib.h>
#include <vector>


#define 	SIMPLE_RECORD_SIZE		1000

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

    extern OneDimTable<SimpleRecord> 			*s_simple_table;

    extern void
    do_simple_init(uint32_t num_records);

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
}

namespace shopping {

    struct ShoppingData {
        uint32_t value[SIMPLE_RECORD_SIZE/4];
        
        ShoppingData() {
            for (size_t i = 0; i < SIMPLE_RECORD_SIZE/4; ++i) {
                value[i] = (uint32_t)rand();
            }
        }
        
        void
        UpdateRead(uint32_t *read) {
            for (size_t i = 0; i < SIMPLE_RECORD_SIZE/4; ++i) {
                value[i] += read[i];
            }
        }
        
        void
        Update() {
            for (size_t i = 0; i < SIMPLE_RECORD_SIZE/4; ++i) {
                value[i] += 1;
            }
        }
    };

    void
    do_shopping_init(uint32_t num_customers, uint32_t num_items);
    
    extern OneDimTable<ShoppingData> 			*s_item_table;
    extern OneDimTable<ShoppingData> 			*s_cart_table;

    class AddItemAction : public Action {
    public:
        std::vector<uint64_t> items;
        AddItemAction() {
            materialize = false;
            is_blind = false;
        }

        virtual bool
        NowPhase() {
            return true;
        }
        
        virtual void
        LaterPhase() {
            uint32_t read_data[250];
            memset(read_data, 0, 1000);
            for (size_t i = 0; i < items.size(); ++i) {
                ShoppingData *item = s_item_table->GetPtr(items[i]);
                for (int j = 0; j < 250; ++j) {
                    read_data[j] += item->value[j];
                }
            }

            ShoppingData *cart = s_cart_table->GetPtr(writeset[0].record.m_key);
            cart->UpdateRead(read_data);
        }
    };

    class EagerAddItemAction : public EagerAction {
    public:
        std::vector<uint64_t> items;
        virtual void
        Execute() {
            uint32_t read_data[250];
            memset(read_data, 0, 1000);
            for (size_t i = 0; i < items.size(); ++i) {
                ShoppingData *item = s_item_table->GetPtr(items[i]);
                for (int j = 0; j < 250; ++j) {
                    read_data[j] += item->value[j];
                }
            }
            ShoppingData *cart = s_cart_table->GetPtr(writeset[0].record.m_key);
            cart->UpdateRead(read_data);
        }
    };


    class Checkout : public Action {
    public:
        Checkout() {
            materialize = true;
            is_blind = false;
        }

        virtual bool
        NowPhase() {
            return true;
        }

        virtual void
        LaterPhase() {
            ShoppingData *cart = s_cart_table->GetPtr(writeset[0].record.m_key);
            for (size_t i = 1; i < writeset.size(); ++i) {
                ShoppingData *item = s_item_table->GetPtr(writeset[i].record.m_key);
                item->UpdateRead(cart->value);
            }
            cart->Update();
        }
    };

    
    class EagerCheckout : public EagerAction {
    public:
        virtual void
        Execute() {
            ShoppingData *cart = s_cart_table->GetPtr(writeset[0].record.m_key);
            for (size_t i = 1; i < writeset.size(); ++i) {
                ShoppingData *item = s_item_table->GetPtr(writeset[i].record.m_key);
                item->UpdateRead(cart->value);
            }
            cart->Update();
        }
    };

    class ClearCart : public Action {
        uint32_t *to_write;
    public:
        ClearCart(uint32_t *input) {
            materialize = true;
            is_blind = true;
            to_write = input;
        }

        virtual bool
        NowPhase() {
            return true;
        }
        
        virtual void
        LaterPhase() {
            ShoppingData *record = s_cart_table->GetPtr(writeset[0].record.m_key);
            memcpy(record->value, to_write, 1000);
        }
    };
    
    class EagerClearCart : public EagerAction {
        uint32_t *to_write;
    public:
        EagerClearCart(uint32_t *input) {
            to_write = input;
        }
        
        virtual void
        Execute() {
            ShoppingData *record = s_cart_table->GetPtr(writeset[0].record.m_key);
            memcpy(record->value, to_write, 1000);
        }
    };
}

#endif		//  SIMPLE_ACTION_HH_
