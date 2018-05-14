package org.muks.insider.utils;

//import com.datastax.spark.connector.cql.CassandraConnector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.muks.insider.businessobjects.TempRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;


public class Utils {
    private static Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     * Converts json to a Dataset.  Based on the file contained in src/test/resources/sample.json
     *
     * @param stream
     */
    public static void runAnalyticsBySparkAPI(JavaInputDStream<ConsumerRecord<String, String>> stream) {

        /** caching the stream data is required as at times
         *  - spark reads from the same kafka topic thread to dump to console, write or even a count leading to ConcurrentModification exception.
         *
         *  Importantly, leads to memory overload too.
         *  */
        stream.cache();

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
                LOG.info("=== TS: " + time + "====");

                if (parsedJson.count() > 0) {   /** if the read line is non-empty, proceed.... */

                    if (SparkUtils.checkColumnInDataset(parsedJson, "user_id")) {
                        /** Logic for user purchase affinity
                         *      - flatten the entire json into - user_id, session_id, cart_amount and product-details
                         *              - where cart_amount > 0
                         *      - group by user_id, session_id, sum(cart_amount)
                         *      - create a product_id collections list for analytics
                         */
                        Dataset purchaseDataWithProductFlat = parsedJson.select(
                                parsedJson.col("user_id"),
                                parsedJson.col("session_id"),
                                parsedJson.col("cart_amount").cast(DataTypes.DoubleType).as("cart_amount"),
                                org.apache.spark.sql.functions.explode(parsedJson.col("products")).as("products_flat"))
                                .where(parsedJson.col("cart_amount").isNotNull()
                                        .or(parsedJson.col("cart_amount").notEqual("")
                                        )
                                );

                        Dataset purchaseDataFlattened = purchaseDataWithProductFlat.select(
                                purchaseDataWithProductFlat.col("user_id"),
                                purchaseDataWithProductFlat.col("session_id"),
                                purchaseDataWithProductFlat.col("cart_amount").cast(DataTypes.DoubleType).as("cart_amount"),
                                org.apache.spark.sql.functions.explode(
                                        purchaseDataWithProductFlat.col("products_flat.category")).as("product_category"),
                                purchaseDataWithProductFlat.col("products_flat.id").as("product_id"),
                                purchaseDataWithProductFlat.col("products_flat.imgUrl").as("imgurl"),
                                purchaseDataWithProductFlat.col("products_flat.name").as("product_name"),
                                purchaseDataWithProductFlat.col("products_flat.price").cast(DataTypes.DoubleType),
                                purchaseDataWithProductFlat.col("products_flat.url").as("product_url")
                        );

                        /**
                         * Crunch the analytics data by having a column exploded with the product-ids
                         */
                        Dataset<Row> purchaseAffinityDataAggregated
                                = purchaseDataFlattened
                                .groupBy("user_id", "session_id")
                                //.groupBy("user_id", "session_id", "product_category")
                                .agg(
                                        sum(purchaseDataFlattened.col("cart_amount")).as("cart_amount"),
                                        sum(purchaseDataFlattened.col("price")).as("product_price"),
                                        concat_ws(",", collect_list(col("product_id"))).as("product_ids")
                                );

                        DateUtils.DateTimeItems datetime = new DateUtils().currentTimestampItems();
                        Dataset<Row> purchaseAffinityDataAggreWithTimeMeta
                                = purchaseAffinityDataAggregated
                                .withColumn("year", functions.lit(datetime.YEAR))
                                .withColumn("month", functions.lit(datetime.MONTH))
                                .withColumn("day", functions.lit(datetime.DAY))
                                .withColumn("hour", functions.lit(datetime.HOURS))
                                .withColumn("min", functions.lit(datetime.MINUTES));

                        LOG.info("Count of the ProductFlattened:= " + purchaseDataWithProductFlat.count());
                        LOG.info("Count of the FullExploded:= " + purchaseDataFlattened.count());
                        LOG.info("Count of the Purchase Affinity Data:= " + purchaseAffinityDataAggreWithTimeMeta.count());

                        purchaseAffinityDataAggreWithTimeMeta.printSchema();
                        purchaseAffinityDataAggreWithTimeMeta.show();

                        /* write to cassandra */
                        SparkDbConnectors.datasetToCassandra(
                                purchaseAffinityDataAggreWithTimeMeta,
                                "insider",
                                "purchase_affinity",
                                SaveMode.Append);


                        /**
                         * Crunch the analytics data by having a column exploded with the product-ids
                         */
                        Dataset<Row> purchaseDataExplode
                                = purchaseDataFlattened
                                .groupBy("user_id", "session_id", "product_category", "product_id")
                                .agg(
                                        sum(purchaseDataFlattened.col("cart_amount")).as("cart_amount"),
                                        sum(purchaseDataFlattened.col("price")).as("product_price")
                                );

                        datetime = new DateUtils().currentTimestampItems();
                        Dataset<Row> purchaseDataExplodeWithMeta
                                = purchaseDataExplode
                                .withColumn("year", functions.lit(datetime.YEAR))
                                .withColumn("month", functions.lit(datetime.MONTH))
                                .withColumn("day", functions.lit(datetime.DAY))
                                .withColumn("hour", functions.lit(datetime.HOURS))
                                .withColumn("min", functions.lit(datetime.MINUTES));


                        LOG.info("Purchase Data::= " + purchaseDataExplode.count());
                        purchaseDataExplode.printSchema();
                        purchaseDataExplodeWithMeta.show();

                        /* write to cassandra */
                        SparkDbConnectors.datasetToCassandra(
                                purchaseDataExplodeWithMeta,
                                "insider",
                                "product_affinity",
                                SaveMode.Append);

                    }
                }

            }
        });

        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                LOG.info("Offset Ranges: {} ", offsetRanges);
            // some time later, after outputs have completed
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });
    }


    /**
     * Converts json to a Dataset.  Based on the file contained in src/test/resources/sample.json
     *
     * @param stream
     */
    public static void runAnalyticsbySparkSQL(JavaInputDStream<ConsumerRecord<String, String>> stream) {
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


                /** By the method of Spark.SQL over a view. Commenting and retaining it for future reference */
                if (parsedJson.count() > 0) {
                    /** caching data set for faster execution, cache it at memory only */
                    parsedJson.persist(StorageLevel.MEMORY_AND_DISK());

                    /** drop the tmp table view as one might end up with exception on view already exists. */
                    sparkSession.catalog().dropTempView(viewName);

                    try {
                        parsedJson.createTempView(viewName);
                    } catch (AnalysisException e) {
                        LOG.warn("AnalysisException", e);
                    }

                    Dataset<Row> sqlDataset = sparkSession.sql(purchaseAffinityQuery);
                    //sqlDataset.write().mode(SaveMode.Overwrite).json(queryOutput);
                    sqlDataset.show();

                    /** drop the tmp table view as one might end up with exception on view already exists. */
                    sparkSession.catalog().dropTempView(viewName);
                }


                /**
                 * I am trying to look for all user-ids who have added some items to cart, in other terms the cart-value being > 0
                 */
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


                    Dataset fullyExploded = productFlattened.select(
                            productFlattened.col("user_id"),
                            sum(productFlattened.col("cart_amount")),
                            org.apache.spark.sql.functions.explode(
                                    productFlattened.col("products_flat.category")).as("product_category"),
                            productFlattened.col("products_flat.id"),
                            productFlattened.col("products_flat.imgUrl").as("imgurl"),
                            productFlattened.col("products_flat.name"),
                            productFlattened.col("products_flat.price"),
                            productFlattened.col("products_flat.url")
                    );


                    LOG.info("Count of the RECORDS:= " + fullyExploded.count());
                    fullyExploded.printSchema();

                    LOG.info("Fully exploded show() -> ");
                    fullyExploded.show();

                    /** write to cassandra */
                    SparkDbConnectors.datasetToCassandra(fullyExploded, "insider", "insider", SaveMode.Append);
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


    /**
     * Generates a DataFrame/Dateset from a stream with window of length .  The result is based on all the
     * data received for the window, not just the duration.
     *
     * @param stream
     * @param windowLength
     */
    private static void windowedProcessing(JavaInputDStream<ConsumerRecord<String, String>> stream, int windowLength) {
        JavaPairDStream<String, String> pairs = stream
                .mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        return new Tuple2<>(record.key(), record.value());
                    }
                });

        JavaPairDStream<String, String> windowedStream = pairs.window(Durations.seconds(windowLength));
        windowedStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, String>, Time>() {

            @Override
            public void call(JavaPairRDD<String, String> rdd, Time time) throws Exception {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

                JavaRDD<TempRow> rowRDD = rdd.map(new Function<Tuple2<String, String>, TempRow>() {

                    @Override
                    public TempRow call(Tuple2<String, String> v1) throws Exception {
                        TempRow row = new TempRow();
                        row.setLine2(v1._2); // This is the json text.
                        return row;
                    }

                });

                Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, TempRow.class);
                //Can also create a createGlobalTempView, shared by all sessions and kept alive until
                //the Spark application terminates.
                dataFrame.createOrReplaceTempView("messages");
                //result is for the window, not just the duration.
                //Note that a Dataset<Row> is a DataFrame.
                //The name of the column is derived from the bean definition.
                Dataset<Row> count = spark.sql("select count(*) from messages");
                Dataset<Row> raw = spark.sql("select * from messages");
                System.out.println("========= " + time + "=========");
                raw.show();
                count.show();
            }
        });

    }


    /**
     * Generates a DataFrame/Dateset from a stream without windowing enabled.  The result
     * is the data received during the current duration.
     *
     * @param stream
     */
    private static void nonWindowedProcessing(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {

            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd, Time time) throws Exception {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

                JavaRDD<TempRow> rowRDD = rdd.map(new Function<ConsumerRecord<String, String>, TempRow>() {

                    @Override
                    public TempRow call(ConsumerRecord<String, String> v1) throws Exception {
                        TempRow row = new TempRow();
                        row.setLine2(v1.value()); // This is the json text.
                        return row;
                    }

                });

                Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, TempRow.class);
                dataFrame.createOrReplaceTempView("messages");

                //result is only for the current duration.
                Dataset<Row> count = spark.sql("select count(*) from messages");
                System.out.println("========= " + time + "=========");
                count.show();
            }
        });
    }
}
