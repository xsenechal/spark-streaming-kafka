
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by xs on 02/03/2017.
 */
public class SparkStream {

    private static final int INTERVAL = 5000;

    public static void main(String[] args) throws InterruptedException {
        SparkStream main = new SparkStream();
        main.createJavaStreamingDStream();
    }

    private void createJavaStreamingDStream() throws InterruptedException {

        // =========================================
        // init Streaming context
        // =========================================
        SparkConf conf = new SparkConf().setMaster("local").setAppName("kafka-to-spark application");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(INTERVAL));


        // =========================================
        // Data input - get data from kafka topic
        // =========================================
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );




        // =========================================
        // Data process part - print every line
        // =========================================
        stream.map(record->(record.value())).print();

        ssc.start();
        ssc.awaitTermination();
    }
}
