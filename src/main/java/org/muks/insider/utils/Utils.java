package org.muks.insider.utils;

//import com.datastax.spark.connector.cql.CassandraConnector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

//import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
//import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

public class Utils {
    private static Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     * Converts json to a Dataset.  Based on the file contained in src/test/resources/sample.json
     *
     * @param stream
     */
    public static void convertJsonToDataset(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        String viewName = "MyView";
        String purchaseAffinityQuery
                = "select user_id, cart_amount, " +
                " from " + viewName +
                " where product IS NOT NULL";


        JavaDStream<String> jsonText = stream
                .map(new Function<ConsumerRecord<String, String>, String>() {
                    @Override
                    public String call(ConsumerRecord<String, String> record) {
                        return record.value();
                    }
                });

        SparkSession sparkSession = JavaSparkSessionSingleton.getInstance(stream.context().conf());

        jsonText.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> text, Time time) throws Exception {
                Dataset<Row> parsedJson = sparkSession.read().json(text);
                LOG.info("========= " + time + "=========");


                /** By the method of Spark.SQL over a view */

//                if (parsedJson.count() > 0) {
//                    /** caching data set for faster execution, cache it at memory only */
//                    parsedJson.persist(StorageLevel.MEMORY_AND_DISK());
//
//                    /** drop the tmp table view as one might end up with exception on view already exists. */
//                    sparkSession.catalog().dropTempView(viewName);
//
//                    try {
//                        parsedJson.createTempView(viewName);
//                    } catch (AnalysisException e) {
//                        LOG.warn("AnalysisException", e);
//                    }
//
//                    Dataset<Row> sqlDataset = sparkSession.sql(purchaseAffinityQuery);
//                    //sqlDataset.write().mode(SaveMode.Overwrite).json(queryOutput);
//                    sqlDataset.show();
//
//                    /** drop the tmp table view as one might end up with exception on view already exists. */
//                    sparkSession.catalog().dropTempView(viewName);
//                }


                if (parsedJson.count() > 0) {
                    LOG.info("========= " + time + "=========");
                    Dataset productFlattened = parsedJson.select(
                            parsedJson.col("user_id"),
                            parsedJson.col("cart_amount"),
                            org.apache.spark.sql.functions.explode(parsedJson.col("products")).as("products_flat"))
                            .where(parsedJson.col("cart_amount").isNotNull()
                                    .or(parsedJson.col("cart_amount").notEqual("")
                                    )
                            );

                    /** Uncomment below statements to print the raw level table. */
//                    productFlattened.printSchema();
//                    productFlattened.show();
//                    LOG.info("========= " + time + "=========");


                    Dataset fullyExploded = productFlattened.select(
                            productFlattened.col("user_id"),
                            productFlattened.col("cart_amount"),
                            org.apache.spark.sql.functions.explode(
                                    productFlattened.col("products_flat.category")).as("product_category"),
                            productFlattened.col("products_flat.id"),
                            productFlattened.col("products_flat.imgUrl").as("imgurl"),
                            productFlattened.col("products_flat.name"),
                            productFlattened.col("products_flat.price"),
                            productFlattened.col("products_flat.url")
                    );


                    LOG.info(fullyExploded.toDF().toString());
                    fullyExploded.printSchema();

                    LOG.info("Fully exploded show() -> ");
                    fullyExploded.show();

                    //CassandraConnector connector = CassandraConnector.apply(stream.context().sc().getConf());
                    fullyExploded.write().format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
                            .options(new HashMap<String, String>() {
                                {
                                    put("keyspace", "insider");
                                    put("table", "insider");
                                }
                            }).save();

                }
            }
        });
    }


    /**
     * Lazily instantiated singleton instance of SparkSession
     */
    static class JavaSparkSessionSingleton {
        private static transient SparkSession instance = null;

        public static SparkSession getInstance(SparkConf sparkConf) {
            if (instance == null) {
                instance = SparkSession.builder().config(sparkConf).getOrCreate();
            }
            return instance;
        }
    }


    public static void printRDD(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        // Iterate over the stream's RDDs and print each element on console.
        // Have to do element-wise fields since element (ConsumerRecord) isn't
        // serializable.
        stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) pairRDD -> {
            pairRDD.foreach(new VoidFunction<ConsumerRecord<String, String>>() {

                @Override
                public void call(ConsumerRecord<String, String> t) throws Exception {
                    LOG.info(t.key() + "," + t.value());
                }
            });
        });

    }
}
