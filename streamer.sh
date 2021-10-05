cd /var/scratch/ddps2105/ddps1/
sh setup.sh
echo "starting streamer on $1"
/var/scratch/ddps2105/Python-3.9.7/python streamer.py --host $1.cm.cluster