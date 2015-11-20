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
import java.util.Properties;
import java.util.Random;

import io.confluent.support.metrics.collectors.BasicCollector;
import io.confluent.support.metrics.collectors.FullCollector;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.TimeUtils;
import io.confluent.support.metrics.kafka.KafkaUtilities;
import io.confluent.support.metrics.serde.AvroSerializer;
import io.confluent.support.metrics.submitters.ConfluentSubmitter;
import io.confluent.support.metrics.submitters.KafkaSubmitter;
import kafka.server.KafkaServer;

/**
 * Periodically reports metrics collected from a Kafka broker.
 *
 * Metrics are being reported to a Kafka topic within the same cluster and/or to Confluent via the
 * Internet.
 *
 * This class is not thread-safe.
 */
public class MetricsReporter implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(MetricsReporter.class);

  /**
   * Default "retention.ms" setting (i.e. time-based retention) of the support metrics topic. Used
   * when creating the topic in case it doesn't exist yet.
   */
  private static final long RETENTION_MS = 365 * 24 * 60 * 60 * 1000L;

  /**
   * Default replication factor of the support metrics topic. Used when creating the topic in case
   * it doesn't exist yet.
   */
  private static final int SUPPORT_TOPIC_REPLICATION = 3;

  /**
   * Default number of partitions of the support metrics topic. Used when creating the topic in case
   * it doesn't exist yet.
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
  private final KafkaUtilities kafkaUtilities;

  public MetricsReporter(KafkaServer server,
                         Properties serverConfiguration,
                         Runtime serverRuntime) {
    this(server, serverConfiguration, serverRuntime, new KafkaUtilities());
  }

  /**
   * @param server              The Kafka server.
   * @param serverConfiguration The properties this server was created from.
   * @param serverRuntime       The Java runtime of the server that is being monitored.
   * @param kafkaUtilities      An instance of {@link KafkaUtilities} that will be used to perform
   *                            e.g. Kafka topic management if needed.
   */
  public MetricsReporter(KafkaServer server,
                         Properties serverConfiguration,
                         Runtime serverRuntime,
                         KafkaUtilities kafkaUtilities) {
    this.kafkaUtilities = kafkaUtilities;
    customerId = getCustomerId(serverConfiguration);
    TimeUtils time = new TimeUtils();
    if (SupportConfig.isAnonymousCustomerId(customerId)) {
      metricsCollector = new BasicCollector(time);
    } else {
      metricsCollector = new FullCollector(server, serverConfiguration, serverRuntime, time);
    }
    metricsCollector.setRuntimeState(Collector.RuntimeState.Running);

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

  private String getCustomerId(Properties serverConfiguration) {
    String fallbackId = SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;
    String id = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    if (id == null || id.isEmpty()) {
      id = fallbackId;
    }
    if (!SupportConfig.isWellFormedCustomerId(id)) {
      log.error("'{}' is not a valid Confluent customer ID -- falling back to id '{}'", id, fallbackId);
      id = fallbackId;
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
      boolean keepWaitingForServerToStartup = true;
      boolean terminateEarly = false;
      while (keepWaitingForServerToStartup) {
        try {
          long waitTimeMs = addOnePercentJitter(SETTLING_TIME_MS);
          Thread.sleep(addOnePercentJitter(SETTLING_TIME_MS));
          log.info("Waiting {} ms for the monitored broker to finish starting up...", waitTimeMs);

          if (kafkaUtilities.isShuttingDown(server)) {
            keepWaitingForServerToStartup = false;
            terminateEarly = true;
            metricsCollector.setRuntimeState(Collector.RuntimeState.ShuttingDown);
            log.info("Stopping metrics collection prematurely because broker is shutting down");
          } else {
            if (kafkaUtilities.isReadyForMetricsCollection(server)) {
              log.info("Monitored broker is now ready");
              keepWaitingForServerToStartup = false;
            }
          }
        } catch (InterruptedException i) {
          terminateEarly = true;
          metricsCollector.setRuntimeState(Collector.RuntimeState.ShuttingDown);
          Thread.currentThread().interrupt();
        }
      }

      if (terminateEarly) {
        log.info("Metrics collection stopped before it even started");
      } else {
        kafkaUtilities.createTopicIfMissing(server.zkUtils(), supportTopic, SUPPORT_TOPIC_PARTITIONS,
            SUPPORT_TOPIC_REPLICATION, RETENTION_MS);

        log.info("Starting metrics collection from monitored broker...");
        boolean keepRunning = true;
        while (keepRunning) {
          try {
            Thread.sleep(addOnePercentJitter(reportIntervalMs));
            submitMetrics();
          } catch (InterruptedException i) {
            metricsCollector.setRuntimeState(Collector.RuntimeState.ShuttingDown);
            submitMetrics();
            log.info("Stopping metrics collection because the monitored broker is shutting down...");
            keepRunning = false;
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            log.error("Stopping metrics collection from monitored broker: {}", e.getMessage());
            keepRunning = false;
          }
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