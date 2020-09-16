The Python code on this directory was developed using Python 3.7.3. It has not been tested using an earlier version.

It imports these modules: _argparse_, _datetime_, and _psycopg2_ (the driver for PostgreSQL and for YugabuyteDB). The first two are brought by the standard Python environment. But you must install _psycopg2_ explicitly. Use the binary version, thus:

```
# Make sure that you have the latest "pip".
# This is 20.2.3 as of 14-Sep-2020.
pip install -U pip

# Then install "psycopg2-binary"
pip install psycopg2-binary
```
(Instructions are given in the doc [HERE](https://docs.yugabyte.com/latest/quick-start/build-apps/python/ysql-psycopg2/).)

