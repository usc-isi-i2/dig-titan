# dig-titan
Code for loading DIG datasets in the Titan graph database and for doing analyses
## How to Install
### Download Titan 0.5.3 from http://s3.thinkaurelius.com/downloads/titan/titan-0.5.3-hadoop2.zip
### Replace following Jars in lib with /usr/lib/hadoop/client/*.jar that has the same name but different version
#### hadoop-mapreduce-client-core-2.2.0.jar
#### hadoop-yarn-api-2.2.0.jar
#### hadoop-yarn-server-common-2.2.0.jar
#### hadoop-mapreduce-client-jobclient-2.2.0.jar
#### hadoop-annotations-2.2.0.jar
#### hadoop-mapreduce-client-shuffle-2.2.0.jar
#### hadoop-mapreduce-client-common-2.2.0.jar
#### hadoop-yarn-common-2.2.0.jar
#### hadoop-auth-2.2.0.jar
#### hadoop-common-2.2.0.jar
#### hadoop-yarn-server-nodemanager-2.2.0.jar
#### hadoop-yarn-client-2.2.0.jar
#### hadoop-hdfs-2.2.0.jar
#### hadoop-mapreduce-client-app-2.2.0.jar

### Use conf/titan-hbase.properties to connect hbase and elasticsearch

## How to Use
### Location of Titan binaries: /home/frank/titan-0.5.3-hadoop2/
### Run the following shell command: export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
### Execute bin/gremlin.sh
### Follow the documentation, either g = TitanFactory.open('conf/titan-hbase.properties') to create a non-hadoop version of graph, or g = HadoopFactory.open(config_file) to create a hadoop version of graph. 
### Refer to http://s3.thinkaurelius.com/docs/titan/0.5.3/index.html for specific configuration
