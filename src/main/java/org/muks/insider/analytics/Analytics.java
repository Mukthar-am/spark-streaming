package org.muks.insider.analytics;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.muks.insider.cli.ParseCLI;
import org.muks.insider.cli.ParserOptionEntity;
import org.muks.insider.cli.ParserOptions;
import org.muks.insider.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

public class Analytics {
    private static Logger LOG = LoggerFactory.getLogger(Analytics.class);
    private static final Pattern SPACE = Pattern.compile(" ");
    private static String BOOTSTRAP_SERVERS = null;
    private static String TOPICS = null;

    public static void main(String[] args) {
        parseCommandLine(args); /** parse command line arguments */

        LOG.info("Spark stream processing on the broker lists={}, Topics={}", BOOTSTRAP_SERVERS, TOPICS);

        SparkConf sparkConf = getSparkConf();
        try (JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2))) {

            Set<String> topicsSet = new HashSet<>(Arrays.asList(TOPICS.split(",")));
            Map<String, Object> kafkaParams = getKafkaProperties(BOOTSTRAP_SERVERS);

            // Create direct kafka stream with brokers and topics
            JavaInputDStream<ConsumerRecord<String, String>> stream
                    = KafkaUtils.createDirectStream(
                    jStreamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams));


            /** printing each record by considering as RDD */
            //Utils.printRDD(stream);


            boolean processJson = true;
            if (processJson) {
                Utils.runAnalyticsBySparkAPI(stream);
            }


            jStreamingContext.start();

            // Keeps the processing live by halting here unless terminated manually
            try {
                jStreamingContext.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static SparkConf getSparkConf() {
        return
                new SparkConf()
                        .setAppName("InsiderAnalytics")
                        .setMaster("local[*]")
                        .set("spark.local.‌​dir", "/opt/insider/spark-tmp")
                        .set("spark.executor.memory", "8g")
                        .set("spark.ui.port", "4040")
                        .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .set("spark.kryoserializer.buffer", "24")

                        .set("spark.speculation", "true")
                        .set("spark.speculation.interval", "100ms")
                        .set("spark.speculation.quantile", "0.90")
                        .set("spark.speculation.multiplier", "3")
                        .set("spark.sql.crossJoin.enabled", "true")
                        .set("spark.streaming.kafka.consumer.cache.enabled", "true");
    }

    private static Map<String, Object> getKafkaProperties(String bootstrapServer) {
        Map<String, Object> kafkaParams = new HashMap<>();
        //kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("bootstrap.servers", bootstrapServer);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "insidergroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        return kafkaParams;
    }


    /**
     * Utility for parsing command line flags, using apache commons CLI. Stop JVM, if exception.
     */
    private static void parseCommandLine(String[] args) {
        LOG.info("Process start time: ");

        String hlpTxt
                = "java -cp <jar-file> " + Analytics.class.getName()
                + " -brokers <comma separated list of broker hostname> "
                + " -topics <comma separated list of topics>";


        ParseCLI parser = new ParseCLI(hlpTxt);

        List<ParserOptionEntity> parserOptionEntityList = new ArrayList<>();
        parserOptionEntityList.add(new ParserOptionEntity("brokers", "comma separated list of broker hostname.", true));
        parserOptionEntityList.add(new ParserOptionEntity("topics", "comma separated list of topics.", true));


        ParserOptions parserOptions = new ParserOptions(parserOptionEntityList);
        String configFile = null;

        try {
            CommandLine commandline = parser.getCommandLine(args, 2, parserOptions);

            if (commandline.hasOption("brokers"))
                BOOTSTRAP_SERVERS = commandline.getOptionValue("brokers");

            if (commandline.hasOption("topics"))
                TOPICS = commandline.getOptionValue("topics");

        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(0);

        }
    }
}
