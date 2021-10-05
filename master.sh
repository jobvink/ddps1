cd /var/scratch/ddps2105/ddps1/
sh setup.sh
/var/scratch/ddps2105/Python-3.9.7/python main.py --master spark://$1.cm.cluster:7077 --host $1.cm.cluster --storage /var/scratch/ddps2105/results/result