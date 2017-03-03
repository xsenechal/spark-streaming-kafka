
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by xs on 02/03/2017.
 */
public class SparkStream {

    private static final int INTERVAL = 5000;

    private String zookeeper = "localhost:2181";   // hardcode zookeeper
    private String groupId = "test";
    private String topicName = "test";
    private Map<String, Integer> topics = new HashMap<>();

    public static void main(String[] args) {
        SparkStream main = new SparkStream();
        main.createJavaStreamingDStream();
    }

    private void createJavaStreamingDStream() {
        // =========================================
        // init Streaming context
        // =========================================
        SparkConf conf = new SparkConf().setMaster("local").setAppName("kafka-to-spark application");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(INTERVAL));

        // =========================================
        // Data input - get data from kafka topic
        // =========================================
        topics.put(topicName, 1);
        JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(ssc, zookeeper, groupId, topics);

        // =========================================
        // Data process part - print every line
        // =========================================
        kafkaStream.foreach(x -> {
            x.foreach(y -> {
                System.out.println("----- message -----");
                System.out.println("input = " + y._1() + "=>" + y._2());
            });
            return null;
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
