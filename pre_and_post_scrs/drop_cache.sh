#!/usr/bin/bash
echo -e "\n ======= `date`:: Drop-cache script called ...... ======== \n" > /tmp/drop_cache_sh.log
export pip=$1
export ppem=$2
export YBPemFile=$4

echo -e "ssh -i ${ppem} -ostricthostkeychecking=no centos@${pip} \"sudo /usr/bin/cat ${YBPemFile}\" > /tmp/my-yb.pem"
ssh -i ${ppem} -ostricthostkeychecking=no centos@${pip} "sudo /usr/bin/cat ${YBPemFile}" > /tmp/my-yb.pem
chmod 600 /tmp/my-yb.pem
for ybsrv in `echo ${3} | tr "," " "`;
do
   echo "ssh -i /tmp/my-yb.pem -ostricthostkeychecking=no -p 54422 centos@${ybsrv} \"sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\"\" " >> /tmp/drop_cache_sh.log
   nohup /usr/bin/ssh -i /tmp/my-yb.pem -ostricthostkeychecking=no -p 54422 centos@${ybsrv} "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\"" >> /tmp/drop_cache_sh.log
done
echo -e " ============== `sleep 10` Drop-cache done at: `date` ===================  \n" >> /tmp/drop_cache_sh.log
/usr/bin/cat /tmp/drop_cache_sh.log