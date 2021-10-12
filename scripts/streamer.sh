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

echo "starting streamer on $1 (/var/scratch/ddps2105/Python-3.9.7/python streamer.py --host $1.cm.cluster --p-purchase 0.5 --p-ad 0.5 --generators 1)"
/var/scratch/ddps2105/Python-3.9.7/python DataQueue.py --host $1.cm.cluster --p-purchase 0.5 --p-ad 0.5 --generators 1