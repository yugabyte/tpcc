import cmn

WARMUP = 5
NTV = cmn.NrTxnsValues()
# ----------------------------------------------------------------------------------------


class Stmts:
    def __init__(self):
        self.drop_table_tn = "drop table if exists t? cascade"

        # Each table t? will receive only a single row. We use "k serial" because this
        # is usual practice. There' no need to use "cache" to reserve large ranges.
        # We leave "alter sequence... cache 5" in place as harmless reminder of
        # usual good practice (esp. when using a distributed SQL database).
        self.create_table_tn = """
          drop table if exists t? cascade;
          
          create table t?(
          k serial  constraint t?_pk    primary key,
          v int     constraint t?_v_nn  not null);
          
          alter sequence t?_k_seq cache 5;
        """

        # No need to prepare "delete". It isn't timed.
        self.delete_table_tn = "delete from t?"

        self.prepare_insert_table_tn = "prepare insert_t?(int) as insert into t?(v) values($1) returning k"

        self.execute_insert_table_tn = "execute insert_t?(%s::int)"

        self.execute_prepared_inserts = []

        self.prepare_select_table_tn = "prepare select_t?(int) as select v from t? where k = $1"

        self.execute_select_table_tn = "execute select_t?(%s::int)"

        self.execute_prepared_selects = []

        funct_start = """
         create or replace function checksum_from_insert_rows(arr integer[])
           returns int
           volatile
           language plpgsql
          as $body$
          declare
            checksum  int not null := 0;
            val       int not null := 0;
            j         int not null := 0;
            new_k     int not null := 0;
            new_val   int not null := 0;
          begin
            foreach val in array arr loop
              case j"""

        funct_when_fragment = """
                when ? then
                  insert into t?(v) values(val) returning k into new_k;
                  select v into new_val from t? where k = new_k;
                  checksum = checksum + new_val;"""

        create_func_end = """
              end case;
              j := j + 1;
            end loop;
            return checksum;
          end;
          $body$
        """

        self.create_function = funct_start

        for j in range(0, NTV.nr_tables):
            self.create_function = self.create_function + funct_when_fragment.replace("?", f'{j:03}')

        self.create_function = self.create_function + create_func_end
# ------------------------------------------------------------------------------------


def create_all_tables_tn(sess, stmts):

    for j in range(0, NTV.nr_tables):
        sess.execute(stmts.create_table_tn.replace("?", f'{j:03}'))
# ------------------------------------------------------------------------------------


def delete_all_relevant_tables_tn(sess, stmts, n):
    for j in range(0, n):
        sess.execute(stmts.delete_table_tn.replace("?", f'{j:03}'))
# ------------------------------------------------------------------------------------


def prepare_all_sqls_for_tn(sess, stmts):

    for j in range(0, NTV.nr_tables):
        sess.execute(stmts.prepare_insert_table_tn.replace("?", f'{j:03}'))
        stmts.execute_prepared_inserts =                          \
            stmts.execute_prepared_inserts +                      \
            [stmts.execute_insert_table_tn.replace("?", f'{j:03}')]

        sess.execute(stmts.prepare_select_table_tn.replace("?", f'{j:03}'))
        stmts.execute_prepared_selects =                          \
            stmts.execute_prepared_selects +                      \
            [stmts.execute_select_table_tn.replace("?", f'{j:03}')]
# ----------------------------------------------------------------------------------------


def method_sql(sess, params, stmts):
    sess.execute("start transaction isolation level repeatable read")
    checksum = 0
    for j in range(1, params.nr_rows_per_txn + 1):
        # Returns "k" for the inserted row.
        k = 0
        sess.execute(stmts.execute_prepared_inserts[j - 1], (j,))
        rows = sess.cur.fetchall()
        n = 0
        for row in rows:
            n += 1
            assert n == 1, "Unexpected: should be just one row"
            k = row[0]

        # Use the returned "k" to get back the "v" that we just inserted.
        sess.execute(stmts.execute_prepared_selects[j - 1], (k,))
        rows = sess.cur.fetchall()
        n = 0
        for row in rows:
            n += 1
            assert n == 1, "Unexpected: should be just one row"
            checksum = checksum + row[0]

    sess.execute("commit")
    assert checksum == NTV.checksums.get(params.nr_rows_per_txn), "checksum assert failed"
# ----------------------------------------------------------------------------------------


def create_proc(sess, stmts):
    sess.execute(stmts.create_function)
# ----------------------------------------------------------------------------------------


def method_proc(sess, params):
    # NOTE: This comment essay is copied into both "single-table.py" and "many-tables.py".
    # Each of these implements "method_proc()". The implementations are different;
    # but the location of this essay at the start of the procedure, and its text,
    # are the same in both files.
    #
    # Notice that we construct the array literal rather than using Python's native
    # structure (a list) and letting psycopg2 do the job under the covers.
    # This might seem silly. The reason is that tailing the server log to see what
    # comes in over the wire shows that psycopg2 builds the PG array constructor,
    # and not the semantically equivalent literal that you see here.
    # Separate stand-alone tests showed that, when the cardinality is big, passing the
    # value as a literal is measurably faster than passing it as an array constructor.
    # The speed difference will probably drown in the noise with cardinalities of <~100.
    # It turns out that the code to built the literal is no longer than the code to
    # build the Python list. so there's no reason not to choose the faster approach.

    lit = '{'
    for j in range(1, params.nr_rows_per_txn + 1):
        lit += str(j) + ','
    # Strip the final comma.
    lit = lit[:-1] + '}'

    sess.execute("select checksum_from_insert_rows(%s::int[])", (lit,))
    rows = sess.cur.fetchall()
    checksum = 0
    n = 0
    for row in rows:
        n += 1
        assert n == 1, "Unexpected: should be just one row"
        checksum = checksum + row[0]

    assert checksum == NTV.checksums.get(params.nr_rows_per_txn), "checksum assert failed"
# ----------------------------------------------------------------------------------------


def main():
    args = cmn.parse_arguments()
    params = cmn.Params(
        args.db,
        "m",
        args.mode,
        args.report_name,
        args.method,
        args.nr_rows_per_txn,
        args.nr_repeats)

    stmts = Stmts()

    if args.mode == "create_tables":
        sess = cmn.DbSession(params)
        cmn.create_timings_tables_and_views(sess)
        create_all_tables_tn(sess, stmts)
        sess.close()

    elif args.mode == "one_shot":
        sess = cmn.DbSession(params)
        cmn.prepare_sqls(sess)

        if params.method == "sql":
            prepare_all_sqls_for_tn(sess, stmts)
        elif params.method == "proc":
            create_proc(sess, stmts)

        # Warm up simply by running it with
        # the present value of nr_rows_per_txn. No need to time it.
        # Loop over the specified number of repeats.
        for i in range(0, WARMUP):
            delete_all_relevant_tables_tn(sess, stmts, params.nr_rows_per_txn)

            if params.method == "sql":
                method_sql(sess, params, stmts)

            elif params.method == "proc":
                method_proc(sess, params)

        # --------------------------------------------------------------------------------
        # Now do it for real and time it.
        # Loop over the specified number of repeats.
        stop_watch = cmn.StopWatch(sess, params)
        for i in range(0, params.nr_repeats):
            delete_all_relevant_tables_tn(sess, stmts, params.nr_rows_per_txn)

            if params.method == "sql":
                stop_watch.start()
                method_sql(sess, params, stmts)
                stop_watch.stop()

            elif params.method == "proc":
                stop_watch.start()
                method_proc(sess, params)
                stop_watch.stop()

            else:
                assert False, 'Logic error (bad value for "method"). Should be caught by parse_arguments()'
        sess.close()

    elif args.mode == "all_successive":
        sess = cmn.DbSession(params)
        cmn.create_timings_tables_and_views(sess)
        create_all_tables_tn(sess, stmts)
        create_proc(sess, stmts)
        sess.close()

        # Loop over the values of nr_txns_values for "sql"
        params.method = "sql"
        for i in NTV.nr_txns_values:
            sess = cmn.DbSession(params)
            cmn.prepare_sqls(sess)
            prepare_all_sqls_for_tn(sess, stmts)
            params.nr_rows_per_txn = i

            for j in range(0, WARMUP):
                delete_all_relevant_tables_tn(sess, stmts, i)
                method_sql(sess, params, stmts)

            stop_watch = cmn.StopWatch(sess, params)
            for j in range(0, params.nr_repeats):
                delete_all_relevant_tables_tn(sess, stmts, i)
                stop_watch.start()
                method_sql(sess, params, stmts)
                stop_watch.stop()

            sess.close()

        # Loop over the values of nr_txns_values for "proc"
        params.method = "proc"
        for i in NTV.nr_txns_values:
            sess = cmn.DbSession(params)
            cmn.prepare_sqls(sess)
            params.nr_rows_per_txn = i

            for j in range(0, WARMUP):
                delete_all_relevant_tables_tn(sess, stmts, i)
                method_proc(sess, params)

            stop_watch = cmn.StopWatch(sess, params)
            for j in range(0, params.nr_repeats):
                delete_all_relevant_tables_tn(sess, stmts, i)
                stop_watch.start()
                method_proc(sess, params)
                stop_watch.stop()

            sess.close()

        sess = cmn.DbSession(params)
        cmn.do_timings_report(sess, params)
        sess.close()

    elif args.mode == "all_interleaved":
        sess = cmn.DbSession(params)
        cmn.create_timings_tables_and_views(sess)
        cmn.prepare_sqls(sess)
        create_all_tables_tn(sess, stmts)
        prepare_all_sqls_for_tn(sess, stmts)
        create_proc(sess, stmts)

        # Warm up each method simply by running it with
        # the max. value of nr_rows_per_txn. No need to time this.
        for i in range(0, WARMUP):
            params.nr_rows_per_txn = NTV.nr_txns_values[len(NTV.nr_txns_values) - 1]
            for params.method in ["sql", "proc"]:
                delete_all_relevant_tables_tn(sess, stmts, params.nr_rows_per_txn)

                if params.method == "sql":
                    method_sql(sess, params, stmts)

                elif params.method == "proc":
                    method_proc(sess, params)

        # Loop over the values of nr_txns_values
        for i in NTV.nr_txns_values:
            params.nr_rows_per_txn = i

            for m in ["sql", "proc"]:
                params.method = m
                stop_watch = cmn.StopWatch(sess, params)

                for j in range(0, params.nr_repeats):
                    delete_all_relevant_tables_tn(sess, stmts, params.nr_rows_per_txn)

                    if params.method == "sql":
                        stop_watch.start()
                        method_sql(sess, params, stmts)
                        stop_watch.stop()

                    elif params.method == "proc":
                        stop_watch.start()
                        method_proc(sess, params)
                        stop_watch.stop()

        cmn.do_timings_report(sess, params)
        sess.close()

    elif args.mode == "do_timings_report":
        sess = cmn.DbSession(params)
        cmn.do_timings_report(sess, params)
        sess.close()

    else:
        assert False, 'Logic error (bad value for "mode"). Should be caught by parse_arguments()'
# ----------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
