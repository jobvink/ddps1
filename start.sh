export SPARK_HOME=/var/scratch/ddps2105/spark-3.1.1-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo 'loading modules'
module load python/3.6.0
module load prun

echo 'reserving nodes in the cluster'
preserve -# 8 -t 00:01:00

# wait one second for the command to actually reserve the nodes
sleep 5
preserve -llist

worker_list=$(preserve -llist | grep ddps2105 | awk '{print $9,$10,$11,$12,$13,$14,$15,$16}')
read -r -a workers <<< "$worker_list"
echo "reserved the following nodes: ${workers[@]}"

if [ ${#workers[@]} -ne 8 ]; then
  echo 'not enough nodes available'
  exit 1
fi

echo "initializeing master on node ${workers[0]}"
# initialize master
echo "" | ssh "${workers[0]}" $SPARK_HOME/sbin/start-master.sh

echo "initializing workers on nodes "
echo "${workers[@]:1}"
# initialize all the workers
for worker in "${workers[@]:1}"
do
    echo "" | ssh "$worker" $SPARK_HOME/sbin/start-worker.sh spark://"${workers[0]}".cm.cluster:7077
done

echo "" | ssh "${workers[0]}" /var/scratch/ddps2105/Python-3.9.7/python streamer.py &!
sleep 5 # wait for the data streamer to start the generators

mkdir -p "/var/scratch/ddps2105/results"
echo "" | ssh "${workers[0]}" /var/scratch/ddps2105/Python-3.9.7/python main.py --master spark://"${workers[0]}".cm.cluster:7077 --storage "/var/scratch/ddps2105/results"