import cmn

WARMUP = 5
NTV = cmn.NrTxnsValues()
# ----------------------------------------------------------------------------------------

CREATE_FUNCTION = """
 create or replace function checksum_from_insert_rows(arr integer[])
   returns int
   volatile
   language plpgsql
  as $body$
  declare
    checksum  int not null := 0;
    val       int not null := 0;
  begin
    foreach val in array arr loop
      insert into t(v) values(val);
    end loop;
    select sum(v) into checksum from t;
    return checksum;     
  end;
  $body$
"""
# ------------------------------------------------------------------------------------


def create_table_t(sess):
    sess.execute("""
      drop table if exists t cascade;
      
      create table t(
        k  serial  constraint t_pk    primary key,
        v int      constraint t_v_nn  not null);
      
      -- The largest number of rows that will be inserted into t in a single session
      -- occurs with "--mode=all_interleaved". It's
      --
      --   (1 + 2 + 4 + 8 + 16 + 32 + 64 + 128 + 256 + 512) == 1023
      --
      -- So choose 2,000 for the sequence pre-allocation cache.
      alter sequence t_k_seq cache 2000;
      """)
# ----------------------------------------------------------------------------------------


def delete_table_t(sess):

    sess.execute("delete from t")
# ------------------------------------------------------------------------------------


def method_sql(sess, params):
    sess.execute("start transaction isolation level repeatable read")

    for j in range(1, params.nr_rows_per_txn + 1):
        sess.execute("execute insert_t(%s)", (j,))

    sess.execute("execute select_checksum_from_t")
    rows = sess.cur.fetchall()
    checksum = 0
    n = 0
    for row in rows:
        n += 1
        assert n == 1, "Unexpected: should be just one row"
        checksum = row[0]

    sess.execute("commit")
    assert checksum == NTV.checksums.get(params.nr_rows_per_txn), "checksum assert failed"
# ----------------------------------------------------------------------------------------


def create_proc(sess):
    sess.execute(CREATE_FUNCTION)
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
        checksum = row[0]

    assert checksum == NTV.checksums.get(params.nr_rows_per_txn), "checksum assert failed"
# ------------------------------------------------------------------------------------


def main():
    args = cmn.parse_arguments()
    params = cmn.Params(
        args.db,
        "s",
        args.mode,
        args.report_name,
        args.method,
        args.nr_rows_per_txn,
        args.nr_repeats)

    if args.mode == "create_tables":
        sess = cmn.DbSession(params)
        cmn.create_timings_tables_and_views(sess)
        create_table_t(sess)
        sess.close()

    elif args.mode == "one_shot":
        sess = cmn.DbSession(params)
        cmn.prepare_sqls(sess)

        if params.method == "sql":
            sess.execute("prepare insert_t(int) as insert into t(v) values($1::int)")
            sess.execute("prepare select_checksum_from_t as select sum(v) from t")
        elif params.method == "proc":
            create_proc(sess)

        # Warm up simply by running it with
        # the present value of nr_rows_per_txn. No need to time it.
        # Loop over the specified number of repeats.
        for i in range(0, WARMUP):
            delete_table_t(sess)

            if params.method == "sql":
                method_sql(sess, params)

            elif params.method == "proc":
                method_proc(sess, params)

        # --------------------------------------------------------------------------------
        # Now do it for real and time it.
        # Loop over the specified number of repeats.
        stop_watch = cmn.StopWatch(sess, params)
        for i in range(0, params.nr_repeats):
            delete_table_t(sess)

            if params.method == "sql":
                stop_watch.start()
                method_sql(sess, params)
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
        create_table_t(sess)
        create_proc(sess)
        sess.close()

        # Loop over the values of nr_txns_values for "sql"
        params.method = "sql"
        for i in NTV.nr_txns_values:
            sess = cmn.DbSession(params)
            cmn.prepare_sqls(sess)
            sess.execute("prepare insert_t(int) as insert into t(v) values($1::int)")
            sess.execute("prepare select_checksum_from_t as select sum(v) from t")
            params.nr_rows_per_txn = i

            for j in range(0, WARMUP):
                delete_table_t(sess)
                method_sql(sess, params)

            stop_watch = cmn.StopWatch(sess, params)
            for j in range(0, params.nr_repeats):
                delete_table_t(sess)
                stop_watch.start()
                method_sql(sess, params)
                stop_watch.stop()

            sess.close()

        # Loop over the values of nr_txns_values for "proc"
        params.method = "proc"
        for i in NTV.nr_txns_values:
            sess = cmn.DbSession(params)
            cmn.prepare_sqls(sess)
            params.nr_rows_per_txn = i

            for j in range(0, WARMUP):
                delete_table_t(sess)
                method_proc(sess, params)

            stop_watch = cmn.StopWatch(sess, params)
            for j in range(0, params.nr_repeats):
                delete_table_t(sess)
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
        create_table_t(sess)
        sess.execute("prepare insert_t(int) as insert into t(v) values($1::int)")
        sess.execute("prepare select_checksum_from_t as select sum(v) from t")
        create_proc(sess)

        # Warm up each method simply by running it with
        # the max. value of nr_rows_per_txn. No need to time this.
        for i in range(0, WARMUP):
            params.nr_rows_per_txn = NTV.nr_txns_values[len(NTV.nr_txns_values) - 1]
            for params.method in ["sql", "proc"]:
                delete_table_t(sess)

                if params.method == "sql":
                    method_sql(sess, params)

                elif params.method == "proc":
                    method_proc(sess, params)

        # Loop over the values of nr_txns_values
        for i in NTV.nr_txns_values:
            params.nr_rows_per_txn = i

            for m in ["sql", "proc"]:
                params.method = m
                stop_watch = cmn.StopWatch(sess, params)

                for j in range(0, params.nr_repeats):
                    delete_table_t(sess)

                    if params.method == "sql":
                        stop_watch.start()
                        method_sql(sess, params)
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
