/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.tools;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import io.confluent.support.metrics.common.time.TimeUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaMetricsToFile {

  private final ConsumerConnector consumer;

  /**
   * Default constructor
   *
   * @param zookeeper Zookeeper connector string e.g., localhost:2181
   * @param runTimeMs Time this script should run for in milliseconds
   */
  public KafkaMetricsToFile(String zookeeper, int runTimeMs) {
    long unixTime = new TimeUtils().nowInUnixTime();
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", "KafkaSupportGroup-" + unixTime + "-" + new Random().nextInt(100000));
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "250");
    props.put("auto.commit.interval.ms", "1000");
    props.put("consumer.timeout.ms", new Integer(runTimeMs).toString());
    props.put("auto.offset.reset", "smallest");

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  public ConsumerConnector getConsumer() {
    return consumer;
  }

  public List<KafkaStream<byte[], byte[]>> getStreams(String topic) {
    Map<String, Integer> topicCount = new HashMap<>();
    topicCount.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(
        topicCount);
    return consumerStreams.get(topic);
  }

  /**
   * Retrieves the metrics from the provided topic and stores them in a compressed local file.
   *
   * @param topic Kafka topic to read from.  Must not be null or empty.
   * @param outputPath Path to the output file.  Must not be null or empty.
   * @return the number of retrieved metrics submissions.
   */
  public int saveMetricsToFile(String topic, String outputPath) {
    int numMessages = 0;
    Map<String, Integer> topicCount = new HashMap<>();
    FileOutputStream out;

    if (topic == null || topic.isEmpty()) {
      System.err.println("Topic name must be specified");
      return 0;
    }
    if (outputPath == null || outputPath.isEmpty()) {
      System.err.println("Output path must be specified");
      return 0;
    }
    File outFile = new File(outputPath);
    try (FileOutputStream fOut = new FileOutputStream(outFile);
         BufferedOutputStream bOut = new BufferedOutputStream(fOut);
         ZipArchiveOutputStream zOut = new ZipArchiveOutputStream(bOut)) {

      topicCount.put(topic, 1);
      Map<String, List<KafkaStream<byte[], byte[]>>>
          consumerStreams =
          consumer.createMessageStreams(topicCount);
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
      System.err.println("File not found: " + e.getMessage());
      return 0;
    } catch (IOException e) {
      System.err.println("IOException: " + e.getMessage());
      return 0;
    } catch (ConsumerTimeoutException e) {
      System.out.println("Collection completed.");
    }

    if (numMessages == 0) {
      outFile.delete();
      System.out.println("No records found.");
    } else {
      System.out.println("Created file " + outputPath + " with " + numMessages + " records");
    }

    return numMessages;
  }

  public void shutdown() {
    consumer.shutdown();
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: zookeeperServer topic outputFile runtimeSecs");
      return;
    }
    String zookeeper = args[0];
    String topic = args[1];
    String outputPath = args[2];
    int runtimeSeconds = Integer.parseInt(args[3]);
    int runTimeMs = runtimeSeconds * 1000;
    System.out.print("Collecting metrics. This might take up to " + runtimeSeconds + " seconds.");

    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(zookeeper, runTimeMs);
    kafkaMetricsToFile.saveMetricsToFile(topic, outputPath);
    kafkaMetricsToFile.getConsumer().shutdown();
  }
}
