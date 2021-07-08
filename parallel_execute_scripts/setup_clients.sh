set -ex

for n in $(cat clients.txt);
do
	IDX=$(echo $n | cut -d ":" -f 1);
	IP=$(echo $n | cut -d ":" -f 2);
	# Upload new limits.conf.
	scp $SCP_ARGS -ostricthostkeychecking=no limits.conf $SSH_USER@$IP:~
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'sudo cp ~/limits.conf /etc/security/limits.conf'

	# Install TPCC.
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'sudo yum install -y java wget; rm -rf ~/tpcc; rm ~/tpcc.tar.gz; wget https://github.com/yugabyte/tpcc/releases/download/1.4/tpcc.tar.gz; tar -zxvf tpcc.tar.gz'

	# Upload loader and execute scripts.
	#scp $SCP_ARGS -ostricthostkeychecking=no loader$IDX.sh $SSH_USER@$IP:~/loader.sh
	#scp $SCP_ARGS -ostricthostkeychecking=no execute$IDX.sh $SSH_USER@$IP:~/execute.sh

	# Confirm limits are set correctly.
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'ulimit -a'
done
