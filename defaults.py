#Default Params to use
params = {
    'db_url'   : 'localhost',
    'db_type'  : 'mysql',
    'use_mode' : 'perform_joins',
    'database_name' : 'tpch_4g',
    'tables_metadata' : {
            ('region', 'nation_aligned'),
            ('nation_aligned', 'supplier_aligned'),
            ('nation_aligned', 'customer_aligned'),
            ('customer_aligned', 'order_aligned'),
            ('order_aligned', 'lineitem_aligned'),
            ('supplier_aligned', 'partsupp_aligned'),
    }
}
