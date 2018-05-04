package org.muks.insider.analytics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.muks.insider.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

public class Analytics {
    private static Logger LOG = LoggerFactory.getLogger(Analytics.class);
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Analytics <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        //StreamingExamples.setStreamingLogLevels();

        String bootstrapServer = args[0];
        String topics = args[1];

        SparkConf sparkConf = getSparkConf();
        try (JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2))) {

            Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
            Map<String, Object> kafkaParams = getKafkaProperties(bootstrapServer);

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
                Utils.convertJsonToDataset(stream);
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
                        .set("spark.speculation.multiplier", "3");
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
}
