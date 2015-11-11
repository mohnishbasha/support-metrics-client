/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.support.metrics.tools;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.nio.file.Files;
import java.util.Properties;

public class KafkaMetricsToFile {
    private final ConsumerConnector consumer;
    private static String topic;
    private static String zookeeper;
    private static String outputPath;
    private final Integer timeoutMs = new Integer(10000);
    private static File outputFile;

    public KafkaMetricsToFile(String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("consumer.timeout.ms", timeoutMs.toString());
        props.put("auto.offset.reset", "smallest");

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    public void collectMetrics() {
        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            try {
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (it.hasNext()) {
                    Files.write(outputFile.toPath(), it.next().message());
                    System.out.println("Receiving messages");
                }
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
        outputFile = new File(outputPath);

        KafkaMetricsToFile consumer = new KafkaMetricsToFile("KafkaSupportGroup");
        consumer.collectMetrics();
    }
}
