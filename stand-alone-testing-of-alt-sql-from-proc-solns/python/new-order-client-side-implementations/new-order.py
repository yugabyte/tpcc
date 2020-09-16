import argparse
import psycopg2
# ----------------------------------------------------------------------------------------


def parse_arguments():

    parser = argparse.ArgumentParser("Basic SQL Demo")

    parser.add_argument(
        "--db",
        choices=['yb', 'pg'],
        default="pg",
        help="Database: yb to use Yugabyte, pg to use Postgres")

    parser.add_argument(
        "--mode",
        choices=[
          "time_insert_order_line_set_based",
          "time_update_stock_set_based",
          "do_new_order_for_loop_once",
          "do_new_order_pure_sql_once"],
        help="choose the desired action")

    return parser.parse_args()
# ----------------------------------------------------------------------------------------


class Params:
    # Attributes db
    def __init__(self, db):
        self.db = db
# ----------------------------------------------------------------------------------------


class DbSession:
    def __init__(self, params):
        self.db = params.db

        if self.db == 'yb':
            port = "5433"
        elif self.db == 'pg':
            port = "5432"
        else:
            assert False, "Bad db"

        connect_str = "host=localhost dbname=demo user=u1 port=" + port
        self.session = psycopg2.connect(connect_str)
        self.session.set_session(isolation_level='repeatable read', autocommit=True)
        self.cur = self.session.cursor()

    def execute(self, statement, the_vars=None):
        try:
            self.cur.execute(statement, the_vars)

        except psycopg2.DatabaseError as error:
            self.cur.execute("rollback")
            print(error.pgcode)
            print(error.pgerror)
            print("\nUnexpected SQL error")
            raise

        except Exception:
            self.cur.execute("rollback")
            print("\nUnexpected non-SQL error")
            raise

    def close(self):
        if self.session is not None:
            self.cur.close()
            self.session.close()
            self.session = None
# ----------------------------------------------------------------------------------------


class NewOrderActuals:
    """
    "__init__()" sets the attribute values by executing the "new_order_parameters()" stored proc.
    This in turn depends on the "order_lines()" stored proc. And these two depend in turn on
    some UTDs. The "cr-new-order-parameters.sql" script (URL to follow) does this.

    Notice that the array that represents the items that the customer ordered is set up as its
    literal representation. Separate experiments have shown that this is a more efficient way
    to pass an array value from client to server than to pass the text of the semantically
    equivalent "array[]" constructor expression.
    """


    def __init__(self, sess):
        """
        This query is executed only at program start-up and is executed just once.
        So there's no need for the "prepare-execute" paradigm. The purpose is simply
        to set up a set of typical values for the present customer order.
        """
        sess.execute("""
          with
            v as (
              select new_order_parameters() as no)
          select
            (no).w_id,
            (no).d_id,
            (no).c_id,
            (no).o_all_local,
            (no).ols::text,
            cardinality((no).ols)::int
          from v
        """)
        rows = sess.cur.fetchall()
        n = 0
        for row in rows:
            n += 1
            assert n == 1, "Unexpected: should be just one row"
            self.w_id = row[0]
            self.d_id = row[1]
            self.c_id = row[2]
            self.o_all_local = row[3]
            self.ols = row[4]
            self.cardinality_ols = row[5]

        """
        Populate a Python list of tuples to represent the new customer order
        that the SQL order_lines_t represents.
        """
        unnest_ols = """
          select
            u.i_id,
            u.w_id,
            u.qty
          from unnest(%s::order_lines_t) as u(i_id, w_id, qty)
        """

        sess.execute(unnest_ols, (self.ols,))
        self.ols_list = []
        rows = sess.cur.fetchall()
        for row in rows:
            self.ols_list = self.ols_list + [(row[0], row[1], row[2])]
# ----------------------------------------------------------------------------------------


class Stmts:
    # Dull, but necessary.
    # Issue  # 738 and "ysql_suppress_unsupported_error=true" flag.
    set_min_messages_to_error = "set client_min_messages = error"

    start_txn = "start transaction isolation level repeatable read"
    commit = "commit"

    """
    ----------------------------------------------------------------------------------
    GROUP ONE:
    "NEW_ORDER()" SINGLE-ROW SQLS — COMMON TO "FOR-LOOP" AND "PURE-SQL"
    """

    # The results of executing "select_customer" and "select_warehouse" are never used.
    # But the TPC-C says that they should be.
    # (They're inherited from the University's Java code without being used.)

    prepare_select_customer = """
      prepare select_customer(int, int, int) as
      select
        c_discount,
        c_last::text,
        c_credit::text
      from customer
      where
        c_w_id = $1  and
        c_d_id = $2  and
        c_id   = $3
    """

    execute_select_customer = """
      execute select_customer(%s::int, %s::int, %s::int)
    """

    prepare_select_warehouse = """
      prepare select_warehouse(int) as
      select w_tax
      from warehouse
      where w_id = $1
    """

    execute_select_warehouse = """
      execute select_warehouse(%s::int)
    """

    prepare_select_district = """
      prepare select_district(int, int) as
      select d_next_o_id
      from district
      where
        d_w_id = $1 and
        d_id   = $2
      for update
    """

    execute_select_district = """
      execute select_district(%s::int, %s::int)
    """

    prepare_update_district = """
      prepare update_district(int, int) as
      update district
      set d_next_o_id = d_next_o_id + 1
      where
        d_w_id = $1 and
        d_id   = $2
    """

    execute_update_district = """
      execute update_district(%s::int, %s::int)
    """

    prepare_insert_oorder = """
      prepare insert_oorder(int, int, int, int, int, int) as
      insert into oorder    (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
      values                ($1,   $2,     $3,     $4,     now(),     $5,       $6)
    """

    execute_insert_oorder = """
      execute insert_oorder(%s::int, %s::int, %s::int, %s::int, %s::int, %s::int)
    """

    prepare_insert_new_order = """
      prepare insert_new_order(int, int, int) as
      insert into new_order (no_o_id, no_d_id, no_w_id)
      values                ($1,      $2,      $3)
    """

    execute_insert_new_order = """
      execute insert_new_order(%s::int, %s::int, %s::int)
    """

    """
    ----------------------------------------------------------------------------------
    GROUP TWO:
    "NEW_ORDER()" SINGLE-ROW SQLS. USED ONLY BY "FOR-LOOP".
    """

    prepare_select_item_stock = """
      prepare select_item_stock(int, int, int) as

      select
        i.i_price, s.s_quantity,
        case $1
          when 1 then s.s_dist_01
          when 2 then s.s_dist_02
          when 2 then s.s_dist_02
          when 3 then s.s_dist_03
          when 4 then s.s_dist_04
          when 5 then s.s_dist_05
          when 6 then s.s_dist_06
          when 7 then s.s_dist_07
          when 8 then s.s_dist_08
          when 9 then s.s_dist_09
                 else s.s_dist_10
        end as ol_dist_info

      from item i inner join stock s on i.i_id = s.s_i_id

      where
        i.i_id =   $2 and
        s.s_i_id = $2 and
        s.s_w_id = $3
      for update of s
    """

    execute_select_item_stock = """
      execute select_item_stock(%s::int, %s::int, %s::int)
    """

    prepare_insert_order_line_single_row = """
      -- Python adds a trailing space to the "ol_dist_info" value. You see this in the server log.
      prepare insert_order_line_single_row(int, int, int, int, int, int, int, numeric(6,2), text) as
      insert into order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
      values                 ($1,      $2,      $3,      $4,        $5,      $6,             $7,          $8,        ltrim(rtrim($9)))
    """

    execute_insert_order_line_single_row = """
      execute insert_order_line_single_row(%s::int, %s::int, %s::int, %s::int, %s::int, %s::int, %s::int, %s::numeric(6,2), %s::text)
    """

    prepare_update_stock_single_row = """
      prepare update_stock_single_row(int, int, int, int, int) as
      update stock set
        s_quantity =
          case ($1 - $2 >= 10)
            when true then      $1 - $2
            else           91 + $1 - $2
          end,
        s_ytd        = s_ytd + $2,
        s_order_cnt  = s_order_cnt + 1,
        s_remote_cnt = s_remote_cnt +
          case ($3 = $4)
            when true then 0
            else           1
          end
      where
        s_i_id = $5 and
        s_w_id = $4
    """

    execute_update_stock_single_row = """
      execute update_stock_single_row(%s::int, %s::int, %s::int, %s::int, %s::int)
    """

    """
    ----------------------------------------------------------------------------------
    GROUP THREE:
    "NEW_ORDER()" SET-BASED SQLS USING UNNEST(). USED ONLY BY "PURE-SQL".
    """

    prepare_insert_order_line_set_based = """
      prepare insert_order_line_set_based(int, int, order_lines_t, int) as
      insert into order_line(
        ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
        ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
    
      with
        order_lines as (
          -- The order is important to get the proper correlation
          -- between the order-line number and what item it specified.
          --
          -- It's a property of "unnest()" that it produces the rows
          -- in subscript order, so the empty OVER() is OK here.
          -- If we do away with the WITH view "order_lines" and put
          -- "unnest()" directly in the FROM list, then the result order,
          -- and therefore the "row_number()" values, seem to depend
          -- on the plan selection. And by this point, we can't access
          -- the original array element ordering.
          select
            row_number() over() as j, u.i_id, u.w_id, u.qty
          from unnest($3) as u(i_id, w_id, qty))
      select
        $4, $2, $1,
    
        u.j, u.i_id, u.w_id, u.qty, u.qty*i.i_price,
    
        case $2
          when 1 then s.s_dist_01
          when 2 then s.s_dist_02
          when 2 then s.s_dist_02
          when 3 then s.s_dist_03
          when 4 then s.s_dist_04
          when 5 then s.s_dist_05
          when 6 then s.s_dist_06
          when 7 then s.s_dist_07
          when 8 then s.s_dist_08
          when 9 then s.s_dist_09
                  else s.s_dist_10
        end as ol_dist_info
    
      from
        order_lines as u
        inner join item  as i on u.i_id = i.i_id
        inner join stock as s on i.i_id = s.s_i_id
    """

    execute_insert_order_line_set_based = "execute insert_order_line_set_based(%s::int, %s::int, %s::order_lines_t, %s::int)"

    prepare_update_stock_set_based = """
      prepare update_stock_set_based(int, order_lines_t) as
        with
          v as (
            select
              u.i_id                                      as o_i_id,
              u.w_id                                      as o_w_id,
              u.qty                                       as o_qty,
              case (s.s_quantity - u.qty >= 10)
                when true then      s.s_quantity - u.qty
                          else 91 + s.s_quantity - u.qty
              end                                         as s_qty,
              case (u.w_id = $1)
                when true then 0
                          else 1
              end                                         as delta
    
            from
              unnest($2) as u(i_id, w_id, qty)
              inner join stock s on  s.s_i_id = u.i_id
                                 and s.s_w_id = u.w_id)
    
        update stock set
          s_quantity   = v.s_qty,
          s_ytd        = s_ytd + v.o_qty,
          s_order_cnt  = s_order_cnt + 1,
          s_remote_cnt = s_remote_cnt + v.delta
        from v
        where
          stock.s_i_id = v.o_i_id and
          stock.s_w_id = v.o_w_id
    """

    execute_update_stock_set_based = "execute update_stock_set_based(%s::int, %s::order_lines_t)"
# ----------------------------------------------------------------------------------------


def prepare_all_sqls(sess):
    # Common to both "for-loop" and "pure-sql".
    sess.execute(Stmts.prepare_select_customer)
    sess.execute(Stmts.prepare_select_warehouse)
    sess.execute(Stmts.prepare_select_district)
    sess.execute(Stmts.prepare_update_district)
    sess.execute(Stmts.prepare_insert_oorder)
    sess.execute(Stmts.prepare_insert_new_order)

    # Specific to "for-loop".
    sess.execute(Stmts.prepare_select_item_stock)
    sess.execute(Stmts.prepare_insert_order_line_single_row)
    sess.execute(Stmts.prepare_update_stock_single_row)

    # Specific to "pure-sql".
    sess.execute(Stmts.prepare_insert_order_line_set_based)
    sess.execute(Stmts.prepare_update_stock_set_based)
# ------------------------------------------------------------------------------------


def time_insert_order_line_set_based(sess, noa):
    """
    You must do this by hand:

        alter table order_line drop constraint if exists ol_fkey_o

    by hand before execution this "time_insert_order_line_set_based()".
    This is left as a manual step in case you would regret doing it in the
    env. of a data volume where re-creating it takes appreciable time.
    Having said this, consider saying  "not valid", to speed it up thus:

        alter table only order_line
        add constraint ol_fkey_o foreign key(ol_w_id, ol_d_id, ol_o_id)
        references oorder(o_w_id, o_d_id, o_id)
        not valid

    ALSO: Because this timing experiment uses a crude, but sufficient approach to
    setting the next order number, you must do this at the ysqlsh/psql prompt
    before invoking the program again.

        \i real-data/restore-new_order-oorder-district-to-starting.sql
    """

    sess.execute(Stmts.execute_select_district, (noa.w_id, noa.d_id,))
    rows = sess.cur.fetchall()
    n = 0
    # Avoid spurious "might be referenced before assignment" warning.
    d_next_o_id_v = 0
    for row in rows:
        n += 1
        assert n == 1, "Unexpected: should be just one row"
        d_next_o_id_v = row[0]

    sess.execute(Stmts.start_txn)

    for j in range(0, 5):
        d_next_o_id_v += 1
        sess.execute(Stmts.execute_insert_order_line_set_based,
          (noa.w_id, noa.d_id, noa.ols, d_next_o_id_v,))

    sess.execute(Stmts.commit)
# ------------------------------------------------------------------------------------


def time_update_stock_set_based(sess, noa):
    sess.execute(Stmts.start_txn)

    for j in range(0, 5):
        sess.execute(Stmts.execute_update_stock_set_based, (noa.w_id, noa.ols,))

    sess.execute(Stmts.commit)
# ------------------------------------------------------------------------------------


def d_next_o_id_v_from_common_single_row_sqls(sess, noa):
    """
    Though the results of executing "select_customer" and "select_warehouse" are never used,
    we perform "fetchall()" so's not entirely to cheat on the timing.
    """
    sess.execute(Stmts.execute_select_customer, (noa.w_id, noa.d_id, noa.c_id,))
    rows = sess.cur.fetchall()
    n = 0
    for row in rows:
        n += 1
        assert n == 1, "Unexpected: should be just one row"
        # Do anything just for the sake of it.
        assert row[0] >= 0, "Bad 'row[0] >= 0'"

    sess.execute(Stmts.execute_select_warehouse, (noa.w_id,))
    rows = sess.cur.fetchall()
    n = 0
    for row in rows:
        n += 1
        assert n == 1, 'From "select_customer(), Unexpected: should be just one row'
        # Do anything just for the sake of it.
        assert row[0] >= 0, "Bad 'row[0] >= 0'"

    # Two separate statements rather than "UPDATE... RETURNING" as
    # interim workaround for Issue #5366.
    sess.execute(Stmts.execute_select_district, (noa.w_id, noa.d_id,))
    rows = sess.cur.fetchall()
    # Avoid spurious "might be referenced before assignment" warning.
    d_next_o_id_v = 0
    n = 0
    for row in rows:
        n += 1
        assert n == 1, "Unexpected: should be just one row"
        d_next_o_id_v = row[0]

    sess.execute(Stmts.execute_update_district, (noa.w_id, noa.d_id,))

    sess.execute(Stmts.execute_insert_oorder,
      (d_next_o_id_v, noa.d_id, noa.w_id, noa.c_id, noa.cardinality_ols, noa.o_all_local,))

    sess.execute(Stmts.execute_insert_new_order,
      (d_next_o_id_v, noa.d_id, noa.w_id,))

    return d_next_o_id_v
# ------------------------------------------------------------------------------------


def new_order_for_loop(sess, noa):
    sess.execute(Stmts.start_txn)

    d_next_o_id_v = d_next_o_id_v_from_common_single_row_sqls(sess, noa)

    j = 0
    for ol in noa.ols_list:
        j    += 1
        i_id = ol[0]
        w_id = ol[1]
        qty  = ol[2]

        sess.execute(Stmts.execute_select_item_stock,
          (noa.d_id, i_id, w_id,))
        rows = sess.cur.fetchall()

        n = 0
        # Avoid spurious "might be referenced before assignment" warning.
        i_price_v = 0.00
        s_quantity_v = 0
        ol_dist_info_v = 0
        for row in rows:
            n += 1
            assert n == 1, "Unexpected: should be just one row"
            i_price_v      = row[0]
            s_quantity_v   = row[1]
            ol_dist_info_v = row[2]

        sess.execute(Stmts.execute_insert_order_line_single_row,
            (d_next_o_id_v, noa.d_id, noa.w_id, j, i_id, w_id, qty, qty*i_price_v, ol_dist_info_v,))

        sess.execute(Stmts.execute_update_stock_single_row,
          (s_quantity_v, qty, noa.w_id, w_id, i_id,))

    sess.execute(Stmts.commit)
# ------------------------------------------------------------------------------------


def new_order_pure_sql(sess, noa):
    sess.execute(Stmts.start_txn)

    d_next_o_id_v = d_next_o_id_v_from_common_single_row_sqls(sess, noa)

    sess.execute(Stmts.execute_insert_order_line_set_based,
      (noa.w_id, noa.d_id, noa.ols, d_next_o_id_v,))

    sess.execute(Stmts.execute_update_stock_set_based, (noa.w_id, noa.ols,))

    sess.execute(Stmts.commit)
# ------------------------------------------------------------------------------------


def main():
    args = parse_arguments()
    params = Params(args.db)
    sess = DbSession(params)
    noa = NewOrderActuals(sess)
    sess.execute(Stmts.set_min_messages_to_error)
    prepare_all_sqls(sess)

    if args.mode == "time_insert_order_line_set_based":
        time_insert_order_line_set_based(sess, noa)
    elif args.mode == "time_update_stock_set_based":
        time_update_stock_set_based(sess, noa)
    elif args.mode == "do_new_order_for_loop_once":
        new_order_for_loop(sess, noa)
    elif args.mode == "do_new_order_pure_sql_once":
        new_order_pure_sql(sess, noa)
    else:
        assert False, 'Logic error (bad value for "mode"). Should be caught by parse_arguments()'
    sess.close()
# ----------------------------------------------------------------------------------------


if __name__ == '__main__':
    main()
