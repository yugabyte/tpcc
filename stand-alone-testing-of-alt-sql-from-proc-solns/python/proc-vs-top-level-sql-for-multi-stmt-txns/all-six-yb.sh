python single-table.py --db=yb --mode=create_tables

python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=1   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=2   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=4   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=8   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=16  --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=32  --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=64  --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=128 --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=256 --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=512 --nr_repeats=100

python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=1   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=2   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=4   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=8   --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=16  --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=32  --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=64  --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=128 --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=256 --nr_repeats=100
python single-table.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=512 --nr_repeats=100

python single-table.py --db=yb --mode=do_timings_report

# ------------------------------------------------------------------------------------------------------------

python single-table.py --db=yb --mode=all_successive  --nr_repeats=100
python single-table.py --db=yb --mode=all_interleaved --nr_repeats=100

# ------------------------------------------------------------------------------------------------------------

python many-tables.py --db=yb --mode=create_tables

python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=1   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=2   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=4   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=8   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=16  --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=32  --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=64  --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=128 --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=256 --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=sql  --nr_rows_per_txn=512 --nr_repeats=100

python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=1   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=2   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=4   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=8   --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=16  --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=32  --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=64  --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=128 --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=256 --nr_repeats=100
python many-tables.py --db=yb --mode=one_shot --method=proc --nr_rows_per_txn=512 --nr_repeats=100

python many-tables.py --db=yb --mode=do_timings_report

# ------------------------------------------------------------------------------------------------------------

python many-tables.py --db=yb --mode=all_successive  --nr_repeats=100
python many-tables.py --db=yb --mode=all_interleaved --nr_repeats=100
