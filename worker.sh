cd /var/scratch/ddps2105/ddps1/
sh setup.sh
$SPARK_HOME/sbin/start-worker.sh spark://"$1".cm.cluster:7077 &