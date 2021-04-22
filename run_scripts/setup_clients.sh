for IP in $(cat clients.txt);
do
    #Install packages
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'sudo yum install -y java wget tmux'

	# Install TPCC.
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'rm -rf ~/tpcc; rm -rf ~/tpcc.tar.gz'
#	scp $SCP_ARGS -ostricthostkeychecking=no tpcc.tar.gz $SSH_USER@$IP:~
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'wget https://github.com/yugabyte/tpcc/releases/download/1.9/tpcc.tar.gz'
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'tar -zxvf tpcc.tar.gz'

	# Upload new limits.conf.
	scp $SCP_ARGS -ostricthostkeychecking=no limits.conf $SSH_USER@$IP:~
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'sudo cp ~/limits.conf /etc/security/limits.conf'

	# Confirm limits are set correctly.
	ssh $SSH_ARGS -ostricthostkeychecking=no $SSH_USER@$IP 'ulimit -a'
done
