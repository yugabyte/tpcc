import argparse
import psycopg2
import datetime
# ----------------------------------------------------------------------------------------


class DbSession:
    def __init__(self, params):
        self.session = psycopg2.connect(params.connect_str)
        self.session.set_session(isolation_level="repeatable read", autocommit=True)
        self.cur = self.session.cursor()
        self.cur.execute("set client_min_messages = warning")

    def execute(self, statement, the_vars=None):
        try:
            self.cur.execute(statement, the_vars)

        except psycopg2.DatabaseError as error:
            self.session.rollback()
            print(error.pgcode)
            print(error.pgerror)
            raise

        except Exception:
            self.session.rollback()
            print("\nUnexpected error")
            raise

    def close(self):
        if self.session is not None:
            self.cur.close()
            self.session.close()
            self.session = None
# ----------------------------------------------------------------------------------------


def parse_arguments():
    parser = argparse.ArgumentParser("proc vs top-level SQL elapsed time experiment")

    parser.add_argument(
        "--db",
        choices=["yb", "pg"],
        default="pg",
        help="yb: YugabyteDB, pg: PostgreSQL")

    parser.add_argument(
        "--mode",
        choices=["one_shot", "all_successive", "all_interleaved", "create_tables", "do_timings_report"],
        default="one_shot",
        help="'db' always matters; 'nr_repeats' matters too for 'all_successive, all_interleaved'")

    parser.add_argument(
        "--report_name",
        default="",
        help='optional name suffix for the "timings_report" filename. The default (empty string) means "use system-generated suffix"')

    parser.add_argument(
        "--method",
        choices=["sql", "proc"],
        default="proc",
        help="""
        sql: many individual statements from client;
        proc: stored procedure
        """)

    parser.add_argument(
        "--nr_rows_per_txn",
        default="1",
        help="Number of rows per txn")

    parser.add_argument(
        "--nr_repeats",
        default="1",
        help="Number of repeats")

    return parser.parse_args()
# ----------------------------------------------------------------------------------------


def do_timings_report(sess, params):
    if params.approach == "s":
        fname = "single-table-"
    elif params.approach == "m":
        fname = "many-tables-"
    else:
        assert False, 'logic error: bad "approach"'

    if params.report_name != "":
        fname += params.report_name + ".txt"
    else:
        # When many separate "one_shot" invocations are used, the last step
        # is a dedicated "do_timings_report" invocation.
        mode = params.mode
        if mode == "do_timings_report":
            mode = "all-one-shots"

        # Replace possible arg-name underscores with hyphens for the filename.
        fname += (mode + "-" + params.db + ".txt").replace("_", "-")

    f = open(fname, "w+")

    sess.execute("select t from timings_report()")
    rows = sess.cur.fetchall()
    for row in rows:
        f.write(row[0] + "\n")

    f.close()
# ----------------------------------------------------------------------------------------


class Params:
    def __init__(self, db, approach, mode, report_name, method, nr_rows_per_txn_txt, nr_repeats_txt):
        if db == "pg":
            self.connect_str = "host=localhost dbname=demo user=u2 port=5432"
        elif db == "yb":
            self.connect_str = "host=localhost dbname=demo user=u2 port=5433"

        self.db              = db
        self.approach        = approach
        self.mode            = mode
        self.report_name     = report_name
        self.method          = method
        self.nr_rows_per_txn = int(nr_rows_per_txn_txt, 10)
        self.nr_repeats      = int(nr_repeats_txt, 10)
# ------------------------------------------------------------------------------------


class Stmts:
    start_txn = "start transaction isolation level repeatable read"
    commit = "commit"

    prepare_select_timing_tests_next_k = """
        prepare select_timing_tests_next_k as
        select timing_tests_next_k from timing_tests_pk_values where k = 1 for update;
    """

    execute_select_timing_tests_next_k = "execute select_timing_tests_next_k"

    prepare_update_timing_tests_next_k = """
        prepare update_timing_tests_next_k as
        update timing_tests_pk_values set timing_tests_next_k = timing_tests_next_k + 1;
    """

    execute_update_timing_tests_next_k = "execute update_timing_tests_next_k"

    prepare_insert_timing_tests = """
      prepare insert_timing_tests(int, timestamp, text, text, text, text, int, int) as
      insert into timing_tests(
        k,
        start_timestamp,
        db,
        approach,
        mode,
        method,
        nr_rows_per_txn,
        nr_repeats)
      values(
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8)
      """

    execute_insert_timing_tests = """
      execute insert_timing_tests(%s::int, %s::timestamp, %s::text, %s::text, %s::text, %s::text, %s::int, %s::int)"""

    prepare_select_times_next_k = """
        prepare select_times_next_k as
        select times_next_k from timing_tests_pk_values where k = 1 for update;
    """

    execute_select_times_next_k = "execute select_times_next_k"

    prepare_update_times_next_k = """
        prepare update_times_next_k as
        update timing_tests_pk_values set times_next_k = times_next_k + 1;
    """

    execute_update_times_next_k = "execute update_times_next_k"

    prepare_insert_times = """
      prepare insert_times(int, int, timestamp, numeric) as
      insert into times(
        k,
        timing_tests_k,
        this_timestamp,
        measured_ms)
      values(
        $1,
        $2,
        $3,
        $4)
      """

    execute_insert_times = """
      execute insert_times(%s::int, %s::int, %s::timestamp, %s::numeric) 
    """
# ------------------------------------------------------------------------------------


class StopWatch:
    def __init__(self, sess, params):
        # Formal step: define all instance attributes here.
        self.sess = sess
        self.timing_tests_k = 0
        self.t_start = datetime.datetime.now()

        self.sess.execute(Stmts.start_txn)
        self.sess.execute(Stmts.execute_select_timing_tests_next_k)
        rows = self.sess.cur.fetchall()
        n = 0
        for row in rows:
            n += 1
            assert n == 1, "Unexpected: should be just one row"
            self.timing_tests_k = row[0]
        self.sess.execute(Stmts.execute_update_timing_tests_next_k)
        self.sess.execute(Stmts.commit)

        self.sess.execute(Stmts.execute_insert_timing_tests,
        (
          self.timing_tests_k,
          self.t_start.strftime("%Y-%m-%d %H:%M:%S"),
          params.db,
          params.approach,
          params.mode,
          params.method,
          params.nr_rows_per_txn,
          params.nr_repeats),
        )

    def start(self):
        self.t_start = datetime.datetime.now()

    def stop(self):
        # Record the times in milliseconds
        measured_ms = ((datetime.datetime.now() - self.t_start).total_seconds())*1000.0
        t_now = datetime.datetime.now()

        self.sess.execute(Stmts.start_txn)
        self.sess.execute(Stmts.execute_select_times_next_k)
        times_k = 0
        rows = self.sess.cur.fetchall()
        n = 0
        for row in rows:
            n += 1
            assert n == 1, "Unexpected: should be just one row"
            times_k = row[0]
        self.sess.execute(Stmts.execute_update_times_next_k)
        self.sess.execute(Stmts.commit)

        self.sess.execute(Stmts.execute_insert_times,
          (
            times_k,
            self.timing_tests_k,
            t_now.strftime("%Y-%m-%d %H:%M:%S"),
            measured_ms),
        )
# ----------------------------------------------------------------------------------------


def create_timings_tables_and_views(sess):
    """
    Notice that this sequence if separate SQL statements, separated by semi-colons, is
    effectively a ".sql" script.It could easily be turned into this, and trivial Python code
    could then read it into a single string value to be used, in turn, as the actual for
    "sess,execute()".

    Doing this would make the same reporting features available for use at the psql/ysqlsh prompt
    to report on collected data in a single "timing_tests-times" table pair into which from
    all runs, using both PG and YB could be collected by appropriately mechanized use of
    "copy to" and "copy from".
    """
    sess.execute("""
      drop function  if exists  timings_report()                               cascade;
      drop function  if exists  speed_ratio(text, text, int)                   cascade;
      drop view      if exists  speed_ratios_versus_nr_rows_per_txn            cascade;
      drop view      if exists  avg_and_sddev_timings                          cascade;
      drop view      if exists  raw_timings                                    cascade;
      drop table     if exists  times                                          cascade;
      drop table     if exists  timing_tests                                   cascade;
      drop table     if exists  timing_tests_pk_values                         cascade;

      create table timing_tests_pk_values(
        k int
          constraint timing_tests_pk_values_pk primary key,
        timing_tests_next_k int
          constraint timing_tests_pk_values_timing_tests_next_k_nn not null,
          constraint timing_tests_pk_values_timing_tests_next_k_chk check(timing_tests_next_k > 0),
        times_next_k int
          constraint timing_tests_pk_values_times_next_k_nn not null,
          constraint timing_tests_pk_values_times_next_k_chk check(times_next_k > 0));

      insert into timing_tests_pk_values(k, timing_tests_next_k, times_next_k)
      values                            (1, 1,                   1           );

      create table timing_tests(
        k int
          constraint timing_tests_pk primary key,
        start_timestamp timestamp
          constraint timing_tests_start_timestamp_nn not null,
        db text
          constraint timing_tests_db_nn not null,
        approach text
          constraint timing_tests_approach_nn not null,
        mode text
          constraint timing_tests_mode_nn not null,
        method text
          constraint timing_tests_method_nn not null,
        nr_repeats int
          constraint timing_tests_nr_repeats_nn not null,
          constraint timing_tests_nr_repeats_chk check(nr_repeats > 0),
        nr_rows_per_txn int
          constraint timing_tests_nr_rows_per_txn_nn not null,
          constraint timing_tests_nr_rows_per_txn_chk check(nr_rows_per_txn > 0));

      create unique index timing_tests_db_method_nr_rows_per_txn_unq
      on timing_tests(db, method, nr_rows_per_txn);

      create table times(
        k int
          constraint times_pk primary key,
        timing_tests_k int
          constraint times_timing_tests_k_nn not null,
        this_timestamp timestamp
          constraint times_this_timestamp not null,
        measured_ms numeric
          constraint times_measured_ms_nn not null,
        constraint times_fk foreign key(timing_tests_k) references timing_tests(k));

      create view raw_timings as
      select
        t1.k as t1_k,
        t2.k as t2_k,
        t1.start_timestamp,
        t1.db,
        t1.approach,
        t1.mode,
        t1.method,
        t1.nr_repeats,
        t1.nr_rows_per_txn,
        t2.this_timestamp,
        t2.measured_ms
      from
        timing_tests as t1
        inner join times as t2 on t2.timing_tests_k = t1.k;

      create view avg_and_sddev_timings as
      select
        t1_k,
        start_timestamp,
        db,
        approach,
        mode,
        method,
        nr_repeats,
        nr_rows_per_txn,
        min(this_timestamp)        as min_this_timestamp,
        max(this_timestamp)        as max_this_timestamp,
        avg(measured_ms)              as avg_measured_ms,
        stddev(measured_ms)::numeric  as stddev_measured_ms,
        min(measured_ms)              as min_measured_ms,
        max(measured_ms)              as max_measured_ms
      from raw_timings
      group by
        t1_k, start_timestamp, db, approach, mode, method, nr_repeats, nr_rows_per_txn;

      create function speed_ratio(method_1 in text, method_2 in text, the_nr_rows_per_txn in int)
        returns table(nr_rows_per_txn int, speed_ratio numeric)
        immutable
        language sql
      as $body$
        select the_nr_rows_per_txn,
        (
          (
            select avg_measured_ms
            from avg_and_sddev_timings
            where method = method_1
            and nr_rows_per_txn = the_nr_rows_per_txn
          )
          /
          (
            select avg_measured_ms
            from avg_and_sddev_timings
            where method = method_2
            and nr_rows_per_txn = the_nr_rows_per_txn
          )
        );
      $body$;

      create view speed_ratios_versus_nr_rows_per_txn as
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 1)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 2)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 4)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 8)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 16)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 32)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 64)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 128)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 256)
      union all
      select nr_rows_per_txn, speed_ratio from speed_ratio('sql', 'proc', 512);
      
      create function timings_report()
        returns table(t text)
        immutable
        language plpgsql
      as $body$
      begin
        <<p>>declare
          db          text not null := '';
          approach    text not null := '';
          mode        text not null := '';
          nr_repeats  text not null  := 0;
        begin
          select distinct a.db, a.approach, a.mode, ltrim(a.nr_repeats::text)
          into strict     p.db, p.approach, p.mode, p.nr_repeats
          from avg_and_sddev_timings as a;

          approach :=
            case approach
              when 's' then 'single-table'
              when 'm' then  'many-tables'
            end;

          t := rpad('db:',         12)||db;          return next;
          t := rpad('approach:'  , 12)||approach;    return next;
          t := rpad('mode:'      , 12)||mode;        return next;
          t := rpad('nr_repeats:', 12)||nr_repeats;  return next;
        end p;

        t := ''; return next;

        <<q>>declare
          nr_rows_per_txn     text   not null := '';
          avg_measured_ms     text   not null := '';
          stddev_measured_ms  text   not null := '';
          min_measured_ms     text   not null := '';
          max_measured_ms     text   not null := '';
          n                   int    not null := 0;
          methods constant    text[] not null := array['sql', 'proc'];
          method              text   not null := '';
        begin
          t := 'method  nr_rows_per_txn  avg_measured_ms  stddev_measured_ms  min_measured_ms  max_measured_ms';
          return next;
          t := '------  ---------------  ---------------  ------------------  ---------------  ---------------';
          return next;

          foreach method in array methods loop
            n := 0;
            for
                q.nr_rows_per_txn,
                q.avg_measured_ms,
                q.stddev_measured_ms,
                q.min_measured_ms,
                q.max_measured_ms in (
              select
                lpad(to_char(a.nr_rows_per_txn,       '99999'), 17),
                lpad(to_char(a.avg_measured_ms,    '99990.99'), 15),
                lpad(to_char(a.stddev_measured_ms, '99990.99'), 18),
                lpad(to_char(a.min_measured_ms,    '99990.99'), 15),
                lpad(to_char(a.max_measured_ms,    '99990.99'), 15)
              from avg_and_sddev_timings as a
              where a.method = q.method
              order by a.nr_rows_per_txn)
            loop
              n := n + 1;
              case n = 1
                when true then
                  if method = 'proc' then
                    t := ''; return next;
                  end if;
                  t := rpad(method, 6)||nr_rows_per_txn||'  '||avg_measured_ms||'  '||stddev_measured_ms||'  '||min_measured_ms||'  '||max_measured_ms;
                else
                  t := rpad(' ',    6)||nr_rows_per_txn||'  '||avg_measured_ms||'  '||stddev_measured_ms||'  '||min_measured_ms||'  '||max_measured_ms;
              end case;
              return next;
            end loop;
          end loop;
        end q;

        t := ''; return next;

        <<b>>declare
          nr_rows_per_txn  text not null := '';
          speed_ratio      text not null := '';
        begin
          t := 'nr_rows_per_txn  speed_ratio';  return next;
          t := '---------------  -----------';  return next;

          for b.nr_rows_per_txn, b.speed_ratio in (
            select
              lpad(to_char(a.nr_rows_per_txn,   '9999'), 15),
              lpad(to_char(a.speed_ratio,     '990.99'), 11)
            from speed_ratios_versus_nr_rows_per_txn as a
            order by a.nr_rows_per_txn)
          loop 
            t := nr_rows_per_txn||'  '||speed_ratio; return next;
          end loop;
        end;
      end;
      $body$;
      """)
# ------------------------------------------------------------------------------------


def prepare_sqls(sess):
    sess.execute(Stmts.prepare_select_timing_tests_next_k)

    sess.execute(Stmts.prepare_update_timing_tests_next_k)

    sess.execute(Stmts.prepare_insert_timing_tests)

    sess.execute(Stmts.prepare_select_times_next_k)

    sess.execute(Stmts.prepare_update_times_next_k)

    sess.execute(Stmts.prepare_insert_times)
# ------------------------------------------------------------------------------------


class NrTxnsValues:
    @staticmethod
    def sum_1_though_n(n):
        s = 0
        for j in range(1, n + 1):
            s += j
        return s

    def __init__(self):
        # Populate with 1, 2, 4,... 256, 512, i.e. 2**0 through 2**9
        self.nr_txns_values = []
        for j in range(0, 10):
            self.nr_txns_values += [2**j]

        self.nr_tables = self.nr_txns_values[len(self.nr_txns_values) - 1]

        # Could have calculated the expected checksum on-demand, in
        # "single-table.py and "many-tables.py" using this:
        #
        #   NTV.NrTxnsValues.sum_1_through_n(params.nr_rows_per_txn)
        #
        # But it's more stylish to use a dictionary of pre-computed values.
        self.checksums = {}
        for j in range(0, 10):
            self.checksums[self.nr_txns_values[j]] = NrTxnsValues.sum_1_though_n(self.nr_txns_values[j])
