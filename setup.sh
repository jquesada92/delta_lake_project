sudo apt install software-properties-common -y 

sudo apt-get install ssh

python3 -m pip install delta-spark==3.0.0
cd $HOME
mkdir $HOME/opt/hadoop

wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
rm hadoop-3.3.6.tar.gz
mv hadoop-3.3.6/* $HOME/opt/hadoop
