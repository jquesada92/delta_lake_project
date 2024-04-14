sudo apt install software-properties-common -y 

sudo apt-get install ssh

python3 -m pip install delta-spark==3.0.0
sudo apt-get install speedtest
cd $HOME
mkdir $HOME/opt/hadoop

wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
rm hadoop-3.3.6.tar.gz
mv hadoop-3.3.6/* $HOME/opt/hadoop

wget https://dlcdn.apache.org/hive/hive-4.0.0/apache-hive-4.0.0-bin.tar.gz
tar -xzf apache-hive-4.0.0-bin.tar.gz
