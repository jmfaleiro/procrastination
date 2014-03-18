#include <simple_action.hh>

namespace simple {
    OneDimTable<SimpleRecord> 		*s_simple_table;
    
    void
    do_simple_init(uint32_t num_records) {
        srand(time(NULL));
        s_simple_table = new OneDimTable<SimpleRecord>(num_records);
    }
}

namespace shopping {
    OneDimTable<ShoppingData> 		*s_item_table;
    OneDimTable<ShoppingData>  		*s_cart_table;
    
    void
    do_shopping_init(uint32_t num_customers, uint32_t num_items) {
        srand(time(NULL));
        s_item_table = new OneDimTable<ShoppingData>(num_items);
        s_cart_table = new OneDimTable<ShoppingData>(num_customers);
    }
}
