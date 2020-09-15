drop type   if exists  new_order_parameters_t  cascade;
drop domain if exists  order_lines_t           cascade;
drop domain if exists  int_nn                  cascade;
drop type   if exists  order_line_t            cascade;

create domain int_nn as int
check ((value is not null));

create type order_line_t as (i_id int_nn, w_id int_nn, qty int_nn);

create domain order_lines_t order_line_t[]
check ((cardinality(value) > 0));

create type new_order_parameters_t as (
  w_id         int_nn,
  d_id         int_nn,
  c_id         int_nn,
  o_all_local  int_nn,
  ols          order_lines_t);

create or replace function order_lines()
  returns order_lines_t
  immutable
  language sql
as $body$
select array[
    (17012, 1, 5),
    (24685, 1, 4),
    (  854, 1, 1),
    (57757, 1, 7),
    (70079, 1, 8),
    (79076, 1, 2),
    (54379, 1, 2),
    (91121, 1, 3),
    (10244, 1, 1),
    (37330, 1, 6)
  ]::order_lines_t;
$body$;

create or replace function new_order_parameters()
  returns new_order_parameters_t
  immutable
  language sql
as $body$
select (1, 4, 21, 1, order_lines())::new_order_parameters_t;
$body$;

create or replace function new_order_parameters_as_report(no new_order_parameters_t)
  returns table (t text)
  immutable
  language plpgsql
as $body$
begin
  t := to_char(no.w_id, '999')                          ||
       to_char(no.d_id, '999')                          ||
       to_char(no.c_id, '999')                          ||
       to_char(no.o_all_local, '999')                   ||

       ' >>'                                            ||

       to_char((no.ols[1]).i_id, '999999')    ||
       to_char((no.ols[1]).w_id, '999')       ||
       to_char((no.ols[1]).qty,  '999')                 ;    return next;

  for j in 2..cardinality(no.ols) loop
    t := lpad(to_char((no.ols[j]).i_id, '999999'), 26)  ||
              to_char((no.ols[j]).w_id, '999')          ||
              to_char((no.ols[j]).qty,  '999')          ;    return next;
  end loop;
end;
$body$;


----------------------------------------------------------------------
/*

\t on
select t from new_order_parameters_as_report(new_order_parameters());
\t off


select '
  (
    1, 4, 21, 1,
      "{
          ""(17012, 1, 5)"",
          ""(24685, 1, 4)"" ,
          ""(  854, 1, 1)"",
          ""(57757, 1, 7)"",
          ""(70079, 1, 8)"",
          ""(79076, 1, 2)"",
          ""(54379, 1, 2)"",
          ""(91121, 1, 3)"",
          ""(10244, 1, 1)"",
          ""(37330, 1, 6)""
      }"
   )
  '::new_order_parameters_t as the_parameters;


*/;
----------------------------------------------------------------------
