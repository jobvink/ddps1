wget https://www.python.org/ftp/python/3.9.7/Python-3.9.7.tgz
tar xzf Python-3.9.7.tgz
cd Python-3.9.7
./configure --enable-optimizations
make
wget https://bootstrap.pypa.io/get-pip.py
./python get-pip.py
rm get-pip.py
python -m pip install -r ../ddps1/requirements.txt
