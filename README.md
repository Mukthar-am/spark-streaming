# spark-streaming
spark kafka based streaming for realtime analytics

### Building spark streaming application
- Clone this repo

        mvn clean install (NO unit tests enabled for now)
        java -cp <spark-streaming-jar> org.muks.analytics.Analytics <list-of-brokers> <list-of-topic>
        java -cp target/<spark-streaming-0.1-SNAPSHOT.jar org.muks.analytics.Analytics localhost:9092 data


### Kafka local setup for the code to work
- Bring up kafka, locally, up and running.
- Create topic = data with 1 replication factor and 1 partition as its for POC use only.
- Kafka processing running on defauls such as broker = localhost:9092

### Cassandra setup details for the application to work
    keyspace = insider
    table = insider

    Table schema ->
    CREATE TABLE insider (
    	user_id TEXT PRIMARY KEY,
    	cart_amount DOUBLE,
    	product_category TEXT,
    	id TEXT,
    	imgurl TEXT,
    	name TEXT,
    	price DOUBLE,
    	url TEXT
    );

### Misc
    SLF4J is used for logging and the logs paths are defauled to /tmp/spark-stream/logs/ with stream.log and test.log (for unit test case logging) purpose.


### Help required?
    If the jar is executed with invalid args, auto exception is throw as below;

        usage: java -cp <jar-file> org.muks.insider.analytics.Analytics -brokers
                    <comma separated list of broker hostname>  -topics <comma
                    separated list of topics>
         -brokers <arg>   comma separated list of broker hostname.
         -help            Help!
         -topics <arg>    comma separated list of topics.
         -version         1.0
