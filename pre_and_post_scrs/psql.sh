#!/usr/bin/bash
echo -e "\n ======= `date`:: Pre-load script called ...... ======== \n" > /tmp/psql_sh.log
for ybsrv in `echo ${1} | tr "," " "`;
do
   echo "PGPASSWORD=${2} nohup /usr/bin/psql -h ${ybsrv} -p 5433 -U yugabyte -c \"SELECT pg_sleep(${3}), 'Pre-LOAD Sleep done...' as msg;\" &" >> /tmp/psql_sh.log
   PGPASSWORD=${2} nohup /usr/bin/psql -h ${ybsrv} -p 5433 -U yugabyte -c "SELECT pg_sleep(${3}), 'Pre-LOAD Sleep done...' as msg;" >> /tmp/psql_sh.log & 2>&1
done
echo -e " ============== `sleep $(( $3 + 10 ))` psql done at: `date` ===================  \n" >> /tmp/psql_sh.log
/usr/bin/cat /tmp/psql_sh.log
