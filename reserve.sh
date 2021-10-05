echo 'loading modules'
module load python/3.6.0
module load prun

echo 'reserving nodes in the cluster'
preserve -# 10 -t 00:15:00