CODE_PATH=/var/scratch/ddps2105/ddps1

export SPARK_HOME=/var/scratch/ddps2105/spark-3.1.1-bin-hadoop3.2

sh setup.sh

echo 'loading modules'
module load python/3.6.0
module load prun

echo 'reserving nodes in the cluster'
preserve -# 10 -t 00:15:00

# wait one second for the command to actually reserve the nodes
sleep 1
preserve -llist

worker_list=$(preserve -llist | grep ddps2105 | awk '{print $9,$10,$11,$12,$13,$14,$15,$16,$17,$18}')
read -r -a workers <<< "$worker_list"
echo "reserved the following nodes: ${workers[@]}"

if [ ${#workers[@]} -ne 10 ]; then
  echo 'not enough nodes available'
  exit 1
fi

echo "initializeing master on node ${workers[0]}"
# initialize master
echo "" | ssh "${workers[0]}" $SPARK_HOME/sbin/start-master.sh

echo "initializing workers on nodes "
echo "${workers[@]:2}"
# initialize all the workers
for worker in "${workers[@]:2}"
do
    echo "" | ssh "$worker" sh $CODE_PATH/worker.sh ${workers[0]} &!
done

echo "starting streamer node ${workers[1]}"
echo "" | ssh "${workers[1]}" sh $CODE_PATH/streamer.sh ${workers[1]} &!
sleep 10 # wait for the data streamer to start the generators

echo "starting master node"
mkdir -p "/var/scratch/ddps2105/results"
echo "" | ssh "${workers[0]}" $CODE_PATH/master.sh "${workers[0]}"