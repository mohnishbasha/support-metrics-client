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
package io.confluent.support.metrics.submitters;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaSubmitter {

  private static final Logger log = LoggerFactory.getLogger(KafkaSubmitter.class);

  private static final Integer requiredNumAcks = 0;
  private static final int retries = 0;
  private static final int retryBackoffMs = 10 * 1000;

  private final String bootstrapServers;
  private final String topic;

  /**
   */
  /**
   * @param bootstrapServers The bootstrap brokers via which we connect to the Kafka cluster to
   *                         which we submit data.  Example: "localhost:9092".
   * @param topic            The Kafka topic to which data is being sent.
   */
  public KafkaSubmitter(String bootstrapServers, String topic) {
    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      throw new IllegalArgumentException("must specify bootstrap servers");
    } else {
      this.bootstrapServers = bootstrapServers;
    }
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("must specify topic");
    } else {
      this.topic = topic;
    }
  }

  /**
   */
  /**
   * Submits data to the configured Kafka topic.  Ignores null inputs.
   *
   * @param bytes The (serialized) data to be sent.  The data is sent as the "value" of a Kafka
   *              message.
   */
  public void submit(byte[] bytes) {
    submit(bytes, createProducer());
  }

  // This method is `protected` instead of `private` to be visible for testing.
  protected void submit(byte[] bytes, Producer<byte[], byte[]> producer) {
    if (bytes != null) {
      Future<RecordMetadata> response =
          producer.send(new ProducerRecord<byte[], byte[]>(topic, bytes));
      producer.close();
      // Block until Kafka acknowledged the receipt of the message
      try {
        if (response != null) {
          response.get();
        } else {
          log.error("Failed to submit metrics to Kafka topic {}: null response", topic);
        }
        log.info("Successfully submitted metrics to Kafka topic {}", topic);
      } catch (InterruptedException e) {
        log.error("Failed to submit metrics to Kafka topic {} (canceled request): {}",
            topic, e.toString());
      } catch (ExecutionException e) {
        log.error("Failed to submit metrics to Kafka topic {} (due to exception): {}",
            topic, e.toString());
      }
    } else {
      log.error("Could not submit metrics to Kafka (metrics data missing)");
    }
  }

  private Producer<byte[], byte[]> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.RETRIES_CONFIG, retries);
    props.put(ProducerConfig.ACKS_CONFIG, Integer.toString(requiredNumAcks));
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    return new KafkaProducer<>(props);
  }

}