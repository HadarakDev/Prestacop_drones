##Install  Kafka on Ubuntu
https://tecadmin.net/install-apache-kafka-ubuntu/

3 Topics:
- Prestacop
- Violation
- NYPD-Csv

sudo systemctl start kafka
sudo systemctl status kafka

#### Create Topic

cd /usr/local/kafka
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Violation

##### Get list
bin/kafka-topics.sh --list --zookeeper localhost:2181