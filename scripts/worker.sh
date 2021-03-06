cd /var/scratch/ddps2105/ddps1/ || exit
chmod +x scripts/*.sh

# configure python
export PYTHONPATH="/var/scratch/ddps2105/Python-3.9.7/python"
export PATH=/var/scratch/ddps2105/Python-3.9.7:$PATH

# configure spark
export SPARK_HOME=/var/scratch/ddps2105/spark-3.1.1-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# configure hadoop
export HADOOP_HOME=/var/scratch/ddps2105/hadoop-3.2.2
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

# configure spark
export SPARK_HOME=/var/scratch/ddps2105/spark-3.1.1-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo "starting worker for spark://$1.cm.cluster:7077 ($SPARK_HOME/sbin/start-worker.sh spark://$1.cm.cluster:7077 &)"
$SPARK_HOME/sbin/start-worker.sh spark://"$1".cm.cluster:7077 &