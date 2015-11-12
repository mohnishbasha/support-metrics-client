package io.confluent.support.metrics.tools;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerTimeoutException;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.util.Random;
import java.util.Properties;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

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


    public void collectMetrics() throws IOException {
        int numMessages = 0;
        Map<String, Integer> topicCount = new HashMap<>();
        FileOutputStream out;
        FileOutputStream fOut = null;
        BufferedOutputStream bOut = null;
        ZipArchiveOutputStream zOut = null;

        try {
            fOut = new FileOutputStream(new File(outputPath));
            bOut = new BufferedOutputStream(fOut);
            zOut = new ZipArchiveOutputStream(bOut);
        } catch (java.io.FileNotFoundException e) {
            System.err.println("File not found: " + e.toString());
            return;
        }

        topicCount.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            try {
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (it.hasNext()) {
                    // keep the data dump in a temporary file initially
                    File tempFile = File.createTempFile(outputPath + numMessages, ".tmp");
                    out = new FileOutputStream(tempFile);
                    out.write(it.next().message());
                    out.close();

                    // add it to a zipped archive
                    ZipArchiveEntry entry = new ZipArchiveEntry(outputPath + "." + numMessages);
                    zOut.putArchiveEntry(entry);
                    IOUtils.copy(new FileInputStream(tempFile.getAbsolutePath()), zOut);
                    zOut.closeArchiveEntry();

                    // delete temporary file
                    tempFile.deleteOnExit();
                    System.out.println("Collecting metric batch #" + numMessages);
                    numMessages++;
                }
            } catch (ConsumerTimeoutException e) {
                System.err.println("Collection completed in " + timeoutMs + " ms");
                break;
            }

        }
        zOut.finish();
        zOut.close();
        bOut.close();
        fOut.close();
        System.out.println("File created is " + outputPath);
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
        try {
            consumer.collectMetrics();
        } catch (Exception e) {
            System.err.println("Exception in collectMetrics: " + e.getMessage());
        }

    }
}
