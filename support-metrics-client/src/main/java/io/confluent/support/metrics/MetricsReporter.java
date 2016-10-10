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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.collectors.CollectorFactory;
import io.confluent.support.metrics.common.CollectorType;
import io.confluent.support.metrics.common.time.TimeUtils;
import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import io.confluent.support.metrics.serde.AvroSerializer;
import io.confluent.support.metrics.submitters.ConfluentSubmitter;
import io.confluent.support.metrics.submitters.KafkaSubmitter;
import io.confluent.support.metrics.utils.Jitter;
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
  public static final int SUPPORT_TOPIC_REPLICATION = 3;

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
  private KafkaSubmitter kafkaSubmitter;
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
   * @param serverConfiguration The properties this server was created from, plus extra Proactive Support (PS) ones
   *                            Note that Kafka does not understand PS properties,
   *                            hence server->KafkaConfig() does not contain any of them, necessitating
   *                            passing this extra argument to the API.
   * @param serverRuntime       The Java runtime of the server that is being monitored.
   * @param kafkaUtilities      An instance of {@link KafkaUtilities} that will be used to perform
   *                            e.g. Kafka topic management if needed.
   */
  public MetricsReporter(KafkaServer server,
                         Properties serverConfiguration,
                         Runtime serverRuntime,
                         KafkaUtilities kafkaUtilities) {
    this.kafkaUtilities = kafkaUtilities;

    if (server == null || serverConfiguration == null || serverRuntime == null || kafkaUtilities == null) {
      throw new IllegalArgumentException("some arguments are null");
    }

    customerId = SupportConfig.getCustomerId(serverConfiguration);
    TimeUtils time = new TimeUtils();
    if (SupportConfig.isAnonymousUser(customerId)) {
      CollectorFactory factory = new CollectorFactory(CollectorType.BASIC, time, server, serverConfiguration, serverRuntime);
      metricsCollector = factory.getCollector();
    } else {
      CollectorFactory factory = new CollectorFactory(CollectorType.FULL, time, server, serverConfiguration, serverRuntime);
      metricsCollector = factory.getCollector();
    }
    metricsCollector.setRuntimeState(Collector.RuntimeState.Running);

    reportIntervalMs = SupportConfig.getReportIntervalMs(serverConfiguration);
    supportTopic = SupportConfig.getKafkaTopic(serverConfiguration);

    if (!supportTopic.isEmpty()) {
      kafkaSubmitter = new KafkaSubmitter(server, supportTopic);
    } else {
      kafkaSubmitter = null;
    }

    String endpointHTTP = SupportConfig.getEndpointHTTP(serverConfiguration);
    String endpointHTTPS = SupportConfig.getEndpointHTTPS(serverConfiguration);
    if (!endpointHTTP.isEmpty() || !endpointHTTPS.isEmpty()) {
      confluentSubmitter = new ConfluentSubmitter(customerId, endpointHTTP, endpointHTTPS);
    } else {
      confluentSubmitter = null;
    }

    if (!reportingEnabled()) {
      log.info("Metrics collection disabled by broker configuration");
    }
    this.server = server;
  }

  protected boolean reportingEnabled() {
    return sendToKafkaEnabled() || sendToConfluentEnabled();
  }

  protected boolean sendToKafkaEnabled() {
    return kafkaSubmitter != null;
  }

  protected boolean sendToConfluentEnabled() {
    return confluentSubmitter != null;
  }

  @Override
  public void run() {
    try {
      if (reportingEnabled()) {
        boolean terminateEarly = waitForServer();
        if (terminateEarly) {
          log.info("Metrics collection stopped before it even started");
        } else {
          log.info("Starting metrics collection from monitored broker...");
          while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(Jitter.addOnePercentJitter(reportIntervalMs));
            submitMetrics();
          }
        }
      }
    } catch (InterruptedException i) {
      metricsCollector.setRuntimeState(Collector.RuntimeState.ShuttingDown);
      submitMetrics();
      log.info("Graceful terminating metrics collection because the monitored broker is shutting down...");
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("Terminating metrics collection from monitored broker because: {}", e.getMessage());
    } finally {
      log.info("Metrics collection stopped");
    }
  }

  /**
   * Waits for the monitored Kafka server to fully start up.
   *
   * This is a blocking call.  This method will return if and only if:
   *
   * <ul> <li>The server has successfully started.  The return value will be false.</li> <li>The
   * server is shutting down.  The return value will be true.</li> <li>The current thread was
   * interrupted.  The return value will be true.</li> </ul>
   */
  private boolean waitForServer() {
    boolean terminateEarly = false;
    try {
      boolean keepWaitingForServerToStartup = true;
      while (keepWaitingForServerToStartup && !Thread.currentThread().isInterrupted()) {
        long waitTimeMs = Jitter.addOnePercentJitter(SETTLING_TIME_MS);
        log.info("Waiting {} ms for the monitored broker to finish starting up...", waitTimeMs);
        Thread.sleep(waitTimeMs);
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
      }
    } catch (InterruptedException i) {
      terminateEarly = true;
      metricsCollector.setRuntimeState(Collector.RuntimeState.ShuttingDown);
      Thread.currentThread().interrupt();
    }
    return terminateEarly;
  }

  // this is a protected method to enable testing
  protected void submitMetrics() {
    byte[] encodedMetricsRecord = null;
    GenericContainer metricsRecord = metricsCollector.collectMetrics();
    try {
      encodedMetricsRecord = encoder.serialize(metricsRecord);
    } catch (IOException e) {
      log.error("Could not serialize metrics record: {}", e.toString());
    }

    try {
      if (sendToKafkaEnabled() && encodedMetricsRecord != null) {
        // attempt to create the topic. If failures occur, try again in the next round, however
        // the current batch of metrics will be lost.
        if (kafkaUtilities.createAndVerifyTopic(server.zkUtils(), supportTopic, SUPPORT_TOPIC_PARTITIONS,
            SUPPORT_TOPIC_REPLICATION, RETENTION_MS)) {
          kafkaSubmitter.submit(encodedMetricsRecord);
        }
      }
    } catch (RuntimeException e) {
      log.error("Could not submit metrics to Kafka topic {}: {}", supportTopic, e.getMessage());
    }

    try {
      if (sendToConfluentEnabled() && encodedMetricsRecord != null) {
        confluentSubmitter.submit(encodedMetricsRecord);
      }
    } catch (RuntimeException e) {
      log.error("Could not submit metrics to Confluent: {}", e.getMessage());
    }
  }

}