create or replace function is_yb()
  returns boolean
  immutable
  language sql
as
$body$
  select
    case version() like '%PostgreSQL 11.2-YB-%'
      when true then true
                else false
    end;
$body$;
