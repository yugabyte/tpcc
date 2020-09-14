\i real-data/tpcc-pg.sql
\i new-order-implementations/cr-new-order-parameters.sql
\i new-order-implementations/cr-new-order-1-sudheer.sql

call new_order(new_order_parameters());

\i real-data-expected-results/cr-insert-data-dump/cr-insert-data-dump.sql
