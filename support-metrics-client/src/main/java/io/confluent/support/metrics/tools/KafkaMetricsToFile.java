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
import java.io.FileNotFoundException;

public class KafkaMetricsToFile {
  private final ConsumerConnector consumer;
  private static final int timeoutMs = 15000;


  public KafkaMetricsToFile(String zookeeper) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", "KafkaSupportGroup" + new Random().nextInt(100000));
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "250");
    props.put("auto.commit.interval.ms", "1000");
    props.put("consumer.timeout.ms", new Integer(timeoutMs).toString());
    props.put("auto.offset.reset", "smallest");

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  public ConsumerConnector getConsumer() {
    return consumer;
  }

  /**
   * Collects the metrics and puts them in a compressed file
   * @param topic Topic of interest, cannot be null or empty
   * @return Number of metric records collected
   * @throws IOException
   */
  public int collectMetrics(String topic, String outputPath){
    int numMessages = 0;
    Map<String, Integer> topicCount = new HashMap<>();
    FileOutputStream out;

    if (topic == null || topic.isEmpty() || outputPath == null || outputPath.isEmpty()) {
      System.err.println("Invalid parameters");
      return 0;
    }

    try (FileOutputStream fOut = new FileOutputStream(new File(outputPath));
         BufferedOutputStream bOut = new BufferedOutputStream(fOut);
         ZipArchiveOutputStream zOut = new ZipArchiveOutputStream(bOut)) {

      topicCount.put(topic, 1);
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
      List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
      for (final KafkaStream stream : streams) {
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
      }
    } catch (FileNotFoundException e) {
      System.err.println("File not found: " + e.toString());
      return 0;
    } catch (IOException e) {
      System.err.println("IOException: " + e.toString());
      return 0;
    } catch (ConsumerTimeoutException e) {
      System.err.println("Collection completed in " + timeoutMs + " ms");
    }

    System.out.println("File name with " + numMessages + " records is " + outputPath);

    return numMessages;
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: zookeeperServer topic outputFile");
      return;
    }
    System.out.print("Collecting metrics. This might take up to " + timeoutMs/1000 + " seconds.");
    String zookeeper = args[0];
    String topic = args[1];
    String outputPath = args[2];


    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(zookeeper);
    try {
      kafkaMetricsToFile.collectMetrics(topic, outputPath);
      kafkaMetricsToFile.getConsumer().shutdown();
    } catch (Exception e) {
      System.err.println("Exception in collectMetrics: " + e.getMessage());
    }

  }
}
