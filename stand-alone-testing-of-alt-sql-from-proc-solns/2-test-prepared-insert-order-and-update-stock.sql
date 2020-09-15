\timing off
-- Issue #738. Up to you if you want to set it back to, say, "warning"
set client_min_messages = error;

--------------------------------------------------------------------------------
-- PREAMBLE.
-- This script assumes that the data, and the code, is already installed.
--
-- Doing only these two "heavy lifting" SQLs in isolation obviously breaks the
-- data-rules. So you have to drop a the "ol_fkey_o" FK constraint for this
-- stand-alone test. It's easy to restore the starting state in the "order_line"
-- table with "delete" because thios sufferst only inserts. But it's impractical
-- to restore the starting state in the "stock" table because this suffers updates
-- to a fixed set of 10 rows.
--
-- Depending on what you want to do next, you might, therefore, need to re-install
-- the starting data before doing that.

-- THE BIND "VARIABLES"
\i new-order-implementations/do-set_bind-variables-from-new-order-parameters.sql
\i real-data/restore-new_order-affected-tables-to-starting-state.sql

-- Each by-hand execution of "insert_order_line"
-- FIRST increments "d_next_o_id".
select (d_next_o_id - 1) as o_id_v
from district
where d_w_id = :w_id_in and d_id = :d_id_in
\gset d_next_

\t on
select ':w_id_in        =  '||:w_id_in::text;
select ':d_id_in        =  '||:d_id_in::text;
select ':d_next_o_id_v  =  '||:d_next_o_id_v::text;

select
  j,
  (:ols_in)[j].i_id,
  (:ols_in)[j].w_id,
  (:ols_in)[j].qty
from generate_subscripts(:ols_in, 1) as g(j);
\t off

\i new-order-implementations/do-print-plans-for-set-based-sqls.sql

/*
________________________________________________________________________________

BEGIN "DO THIS PART BY HAND" to watch it step-by-step.

    alter table order_line drop constraint if exists ol_fkey_o; -- << Look! 

    -- Useful to allow searching in the server log.
    select 'Bllewell: About to test.';
    ____________________________________________________________________________

    -- PART ONE: EXECUTE "insert_order_line".
    -- BEGIN "YOU CAN DO THIS TIME AND AGAIN #1".

        -- Mimic getting the next available order-number
        select (:d_next_o_id_v + 1) as next
        \gset x_
        \set d_next_o_id_v :x_next

        \timing on
        execute insert_order_line(:w_id_in, :d_id_in, :ols_in, :d_next_o_id_v);
        \timing off

        -- SEE WHAT IT DID
        -- Notice that "ol_o_id" is not the leading column in any index on "order_line"
        -- and that "count (distinct ol_w_id)" is 1 (for the value "1").
        -- The plan does a table scan and filter, and the query is slow.
        -- But there's no such query in the bemchmark. This table receives only "insert".
        select
          ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
        from order_line
        where
          ol_w_id = :w_id_in       and
          ol_o_id = :d_next_o_id_v
        order by ol_w_id, ol_d_id, ol_o_id, ol_number;

    END "YOU CAN DO THIS TIME AND AGAIN #1".
    ____________________________________________________________________________

    PART TWO: EXECUTE "insert_order_line".
    BEGIN "YOU CAN DO THIS TIME AND AGAIN #2".

        \timing on
        execute update_stock(:w_id_in, :ols_in);
        \timing off

        select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt
        from stock
        where s_w_id = :w_id_in
        and s_i_id in (select u.i_id from unnest(:ols_in) as u(i_id, w_id, qty))
        order by s_i_id, s_w_id;

    END "YOU CAN DO THIS TIME AND AGAIN #2".

FINALLY
    \i real-data/restore-new_order-affected-tables-to-starting-state.sql
    alter table only order_line
    add constraint ol_fkey_o foreign key(ol_w_id, ol_d_id, ol_o_id)
    references oorder(o_w_id, o_d_id, o_id)
    -- Save time.
    not valid;

END "DO THIS PART BY HAND".
________________________________________________________________________________
*/;
