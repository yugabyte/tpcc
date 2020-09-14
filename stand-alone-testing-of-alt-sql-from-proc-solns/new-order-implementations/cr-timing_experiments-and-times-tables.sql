drop table if exists times;
drop table if exists timing_experiments;

create table timing_experiments(
  k              serial  primary key,
  method         text    not null,
  no_of_repeats  int     not null,
  env            text    not null,
  elapsed_time   int             );

create table times(
  k                     serial   primary key,
  timing_experiments_k  int      not null,
  measured_ms           numeric  not null,

  constraint times_fk foreign key(timing_experiments_k) references timing_experiments(k));
