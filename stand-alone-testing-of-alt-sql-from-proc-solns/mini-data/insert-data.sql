select is_yb() as is_yb
\gset env_

\if :env_is_yb
  \t on
  select 'Installing data for YB';
  \t off
  \ir cr-tables-yb.sql
  \ir insert-data.dump
  \ir cr-indexes-and-constraints-yb.sql

\else
  \t on
  select 'Installing data for PG';
  \t off
  \ir cr-tables-pg.sql
  \ir insert-data.dump
  \ir cr-indexes-and-constraints-pg.sql

\endif
