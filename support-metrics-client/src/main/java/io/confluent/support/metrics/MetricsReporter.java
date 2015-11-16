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
package io.confluent.support.metrics;

import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import io.confluent.support.metrics.collectors.BasicCollector;
import io.confluent.support.metrics.collectors.FullCollector;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.TimeUtils;
import io.confluent.support.metrics.serde.AvroSerializer;
import io.confluent.support.metrics.submitters.ConfluentSubmitter;
import io.confluent.support.metrics.submitters.KafkaSubmitter;
import kafka.admin.AdminUtils;
import kafka.log.LogConfig;
import kafka.admin.AdminOperationException;
import kafka.common.TopicExistsException;
import kafka.server.KafkaServer;
import kafka.server.RunningAsBroker;
import kafka.server.RunningAsController;
import kafka.server.PendingControlledShutdown;
import kafka.server.BrokerShuttingDown;
import kafka.utils.ZkUtils;
import kafka.cluster.Broker;
import scala.collection.JavaConversions;
import scala.collection.Seq;


/**
 * Periodically reports metrics collected from a Kafka broker.
 *
 * Metrics are being reported to a Kafka topic within the same cluster and/or to Confluent via the
 * Internet.
 */
public class MetricsReporter implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(MetricsReporter.class);

  /**
   * Default "retention.ms" setting (i.e. time-based retention) of the support metrics topic.
   * Used when creating the topic in case it doesn't exist yet.
   */
  private static final long RETENTION_MS = 365 * 24 * 60 * 60 * 1000L;

  /**
   * Default replication factor of the support metrics topic.
   * Used when creating the topic in case it doesn't exist yet.
   */
  private static final int SUPPORT_TOPIC_REPLICATION = 3;

  /**
   * Default number of partitions of the support metrics topic.
   * Used when creating the topic in case it doesn't exist yet.
   */
  private static final int SUPPORT_TOPIC_PARTITIONS = 1;

  /**
   * Length of the wait period we give the server to start up completely (in a different thread)
   * before we begin metrics collection.
   */
  private static final long SETTLING_TIME_MS = 10 * 1000L;

  private final String customerId;
  private final long reportIntervalMs;
  private final String supportTopic;
  private final Random random = new Random();
  private final KafkaSubmitter kafkaSubmitter;
  private final ConfluentSubmitter confluentSubmitter;
  private final Collector metricsCollector;
  private final AvroSerializer encoder = new AvroSerializer();
  private final KafkaServer server;

  /**
   * @param server              The Kafka server.
   * @param serverConfiguration The properties this server was created from.
   * @param serverRuntime       The Java runtime of the server that is being monitored.
   */
  public MetricsReporter(KafkaServer server, Properties serverConfiguration, Runtime serverRuntime) {
    customerId = getCustomerId(serverConfiguration);
    TimeUtils time = new TimeUtils();
    if (SupportConfig.isAnonymousCustomerId(customerId)) {
      metricsCollector = new BasicCollector(time);
    } else {
      metricsCollector = new FullCollector(server, serverConfiguration, serverRuntime, time);
    }
    reportIntervalMs = getReportIntervalMs(serverConfiguration);

    supportTopic = getKafkaTopic(serverConfiguration);
    if (!supportTopic.isEmpty()) {
      kafkaSubmitter = new KafkaSubmitter(getKafkaBootstrapServers(server), supportTopic);
    } else {
      kafkaSubmitter = null;
    }

    String endpointHTTP = getEndpointHTTP(serverConfiguration);
    String endpointHTTPS = getEndpointHTTPS(serverConfiguration);
    if (!endpointHTTP.isEmpty() || !endpointHTTPS.isEmpty()) {
      confluentSubmitter = new ConfluentSubmitter(endpointHTTP, endpointHTTPS);
    } else {
      confluentSubmitter = null;
    }

    if (!reportingEnabled()) {
      log.info("Metrics collection disabled by broker configuration");
    }
    this.server = server;
  }

  private void createTopicIfMissing(KafkaServer server, String supportTopic)  {
    int actualReplication = 0;
    ZkUtils zkUtils = server.zkUtils();
    if (AdminUtils.topicExists(zkUtils, supportTopic)) {
      verifySupportTopic();
      return;
    }

    Seq<Broker> brokerList = zkUtils.getAllBrokersInCluster();
    actualReplication = Math.min(SUPPORT_TOPIC_REPLICATION, brokerList.size());
    if (actualReplication < SUPPORT_TOPIC_REPLICATION) {
      log.warn("Creating the support metrics topic {} with a replication factor of {}, which is " +
          "less than the desired replication factor of {} (reason: this cluster contains only {} " +
          "brokers).  If you happen to add more brokers to this cluster, then it is important to " +
          "increase the replication factor of the topic to eventually {} to ensure reliable and " +
          "durable metrics collection.",
          supportTopic, actualReplication, SUPPORT_TOPIC_REPLICATION, brokerList.size(),
          SUPPORT_TOPIC_REPLICATION);
    }
    try {
      Properties metricsTopicProps = new Properties();
      metricsTopicProps.put(LogConfig.RetentionMsProp(), String.valueOf(RETENTION_MS));
      log.info("Attempting to create support metrics topic {} with {} replicas, assuming {} total brokers",
          supportTopic, actualReplication, brokerList.size());
      AdminUtils.createTopic(zkUtils, supportTopic, SUPPORT_TOPIC_PARTITIONS, actualReplication, metricsTopicProps);
    } catch (TopicExistsException te) {
      log.info("Support metrics topic {} already exists", supportTopic);
    } catch (AdminOperationException e) {
      log.error("Could not create support metrics topic {}: {}", supportTopic, e.getMessage());
    }
  }

  private void verifySupportTopic() {
    Set<String> topics = new HashSet<>();
    topics.add(supportTopic);
    scala.collection.Map partitionAssignment = server.zkUtils().getPartitionAssignmentForTopics(
            JavaConversions.asScalaSet(topics).toSeq())
            .get(supportTopic).get();

    int actualNumPartitions = partitionAssignment.size();
    if (actualNumPartitions != SUPPORT_TOPIC_PARTITIONS) {
      log.warn("The support topic {} should have only {} partitions.  Having more partitions " +
          "should not hurt but it is only needed under special circumstances.",
          supportTopic, SUPPORT_TOPIC_PARTITIONS);
    }

    int actualReplication = ((Seq) partitionAssignment.get(0).get()).size();
    if (actualReplication < SUPPORT_TOPIC_REPLICATION) {
      log.warn("The replication factor of the support metrics topic {} is {}, which is less than " +
          "the desired replication factor of {}.  If you happen to add more brokers to this " +
          "cluster, then it is important to increase the replication factor of the topic to " +
          "eventually {} to ensure reliable and durable metrics collection.",
          supportTopic, actualReplication, SUPPORT_TOPIC_REPLICATION, SUPPORT_TOPIC_REPLICATION);
    }
  }

  private String getCustomerId(Properties serverConfiguration) {
    String id = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    if (id == null || id.isEmpty()) {
      id = SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;
    }
    return id;
  }

  private long getReportIntervalMs(Properties serverConfiguration) {
    String intervalString = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG);
    if (intervalString == null || intervalString.isEmpty()) {
      intervalString = SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT;
    }
    try {
      long intervalHours = Long.parseLong(intervalString);
      if (intervalHours < 1) {
        throw new ConfigException(
            SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
            intervalString,
            "Interval must be >= 1");
      }
      return intervalHours * 60 * 60 * 1000;
    } catch (NumberFormatException e) {
      throw new ConfigException(
          SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
          intervalString,
          "Interval is not an integer number");
    }
  }

  private String getKafkaTopic(Properties serverConfiguration) {
    String topic = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    if (topic == null) {
      return "";
    } else {
      return topic;
    }
  }

  private String getKafkaBootstrapServers(KafkaServer server) {
    String hostname = server.config().advertisedHostName();
    Integer port = server.config().advertisedPort();
    return hostname + ":" + port.toString();
  }

  private String getEndpointHTTP(Properties serverConfiguration) {
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "");
  }

  private String getEndpointHTTPS(Properties serverConfiguration) {
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "");
  }

  private boolean reportingEnabled() {
    return sendToKafkaEnabled() || sendToConfluentEnabled();
  }

  private boolean sendToKafkaEnabled() {
    return kafkaSubmitter != null;
  }

  private boolean sendToConfluentEnabled() {
    return confluentSubmitter != null;
  }

  public void run() {
    if (reportingEnabled()) {
      log.info("Waiting for monitored server to start up...");
      while (true) {
        try {
          Thread.sleep(addOnePercentJitter(SETTLING_TIME_MS));

          // if server has suddenly transitioned to shutting down, just exit
          if (server.brokerState().currentState() == PendingControlledShutdown.state() ||
              server.brokerState().currentState() == BrokerShuttingDown.state()) {
            return;
          }

          // if server is running, proceed to collecting metrics
          if (server.brokerState().currentState() == RunningAsBroker.state() ||
              server.brokerState().currentState() == RunningAsController.state()) {
            break;
          }
        } catch (InterruptedException i) {
          // Restore the interrupted status.
          Thread.currentThread().interrupt();
          return;
        }
      }

      createTopicIfMissing(server, supportTopic);

      log.info("Metrics collection started");
      while (true) {
        try {
          Thread.sleep(addOnePercentJitter(reportIntervalMs));
          submitMetrics();
        } catch (InterruptedException i) {
          // Submit a final metrics update before shutdown.
          submitMetrics();
          // Restore the interrupted status.
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          log.info("Terminating metrics collection: {}", e.getMessage());
          break;
        }
      }
    }

    log.info("Metrics collection stopped");
  }

  private long addOnePercentJitter(long reportIntervalMs) {
    return reportIntervalMs + random.nextInt((int) reportIntervalMs / 100);
  }

  private void submitMetrics() {
    byte[] encodedMetricsRecord = null;
    GenericContainer metricsRecord = metricsCollector.collectMetrics();
    try {
      encodedMetricsRecord = encoder.serialize(metricsRecord);
    } catch (IOException e) {
      log.error("Could not serialize metrics record: {}", e.toString());
    }

    try {
      if (sendToKafkaEnabled()) {
        kafkaSubmitter.submit(encodedMetricsRecord);
      }
    } catch (RuntimeException e) {
      log.error("Could not submit metrics to Kafka topic {}: {}", supportTopic, e.toString());
    }

    try {
      if (sendToConfluentEnabled()) {
        confluentSubmitter.submit(encodedMetricsRecord);
      }
    } catch (RuntimeException e) {
      log.error("Could not submit metrics to Confluent: {}", e.toString());
    }
  }
}