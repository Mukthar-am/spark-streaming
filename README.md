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
    CREATE TABLE purchase_affinity (
    	user_id TEXT,
    	session_id TEXT,
    	cart_amount DOUBLE,
    	product_price TEXT,
    	product_ids TEXT,
    	year INT,
    	month INT,
    	day INT,
    	hour INT,
    	min INT,
    	PRIMARY KEY(user_id, month)
    ) WITH CLUSTERING ORDER BY(month DESC);


    Schema for the analytical data;
     |-- user_id: string (nullable = true)
     |-- session_id: string (nullable = true)
     |-- cart_amount: double (nullable = true)
     |-- product_price: double (nullable = true)
     |-- product_ids: string (nullable = false)
     |-- year: integer (nullable = false)
     |-- month: integer (nullable = false)
     |-- day: integer (nullable = false)
     |-- hour: integer (nullable = false)
     |-- min: integer (nullable = false)

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
