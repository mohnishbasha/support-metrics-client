package io.confluent.support.metrics.tools;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.nio.file.Files;
import java.util.Random;
import java.util.Properties;

public class KafkaMetricsToFile {
    private final ConsumerConnector consumer;
    private static String topic;
    private static String zookeeper;
    private static String outputPath;
    private final Integer timeoutMs = new Integer(15000);


    public KafkaMetricsToFile(String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "1000");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("consumer.timeout.ms", timeoutMs.toString());
        props.put("auto.offset.reset", "smallest");

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    public void collectMetrics() {
        int numMessages = 0;
        Map<String, Integer> topicCount = new HashMap<>();
        FileOutputStream out;
        try {
            out = new FileOutputStream(outputPath, true);
        } catch (java.io.FileNotFoundException e) {
            System.err.println(e.toString());
            return;
        }

        topicCount.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            try {
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (it.hasNext()) {
                    out.write(it.next().message());
                    out.write("\n".getBytes());
                    System.out.println("Collecting metric batch #" + numMessages);
                    numMessages++;
                }
                out.close();
            } catch (Exception e) {
                break;
            }

        }

        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: zookeeperServer topic outputFile");
            return;
        }
        zookeeper = args[0];
        topic = args[1];
        outputPath = args[2];


        KafkaMetricsToFile consumer = new KafkaMetricsToFile("KafkaSupportGroup" + new Random().nextInt(100000));
        consumer.collectMetrics();
    }
}
