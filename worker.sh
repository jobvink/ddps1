cd /var/scratch/ddps2105/ddps1/
sh setup.sh

# configure spark
export SPARK_HOME=/var/scratch/ddps2105/spark-3.1.1-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo "starting worker for spark://$1.cm.cluster:7077"
$SPARK_HOME/sbin/start-worker.sh spark://"$1".cm.cluster:7077 &