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

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporter$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.VerifiableProperties;
import scala.Option;
import scala.collection.Seq;

/**
 * Starts a Kafka broker plus an associated "support metrics" collection thread for this broker.
 *
 * This class is similar to Apache Kafka's {@code KafkaServerStartable.scala} but, in addition, it
 * periodically collects metrics from the running broker that are relevant to providing customer
 * support.
 *
 * @see <a href="https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaServerStartable.scala">KafkaServerStartable.scala</a>
 */
public class SupportedServerStartable {

  private static final Logger log = LoggerFactory.getLogger(SupportedServerStartable.class);
  private final KafkaServer server;
  private MetricsReporter metricsReporter = null;
  private Thread metricsThread = null;

  public SupportedServerStartable(Properties brokerConfiguration) {
    Seq<KafkaMetricsReporter> reporters = KafkaMetricsReporter$.MODULE$.startReporters(new VerifiableProperties(brokerConfiguration));
    KafkaConfig serverConfig = KafkaConfig.fromProps(brokerConfiguration);
    Option<String> noThreadNamePrefix = Option.empty();
    server = new KafkaServer(serverConfig, Time.SYSTEM, noThreadNamePrefix, reporters);

    if (SupportConfig.isProactiveSupportEnabled(brokerConfiguration)) {
      try {
        Runtime serverRuntime = Runtime.getRuntime();

        Properties brokerConfigurationPlusMissingPSSettings =
            SupportConfig.mergeAndValidateWithDefaultProperties(brokerConfiguration);

        metricsReporter =
            new MetricsReporter(server, brokerConfigurationPlusMissingPSSettings, serverRuntime);
        metricsThread = newThread("ConfluentProactiveSupportMetricsAgent", metricsReporter, true);
        long reportIntervalMs = SupportConfig.getReportIntervalMs(brokerConfigurationPlusMissingPSSettings);
        long reportIntervalHours = reportIntervalMs / (60 * 60 * 1000);
        // We log at WARN level to increase the visibility of this information.
        log.warn(legalDisclaimerProactiveSupportEnabled(reportIntervalHours));
      } catch (Exception e) {
        // We catch any exceptions to prevent collateral damage to the more important broker
        // threads that are running in the same JVM.
        log.error("Failed to start Proactive Support Metrics agent: {}", e.getMessage());
      }
    } else {
      // We log at WARN level to increase the visibility of this information.
      log.warn(legalDisclaimerProactiveSupportDisabled());
    }
  }

  private static Thread newThread(String name, Runnable runnable, boolean daemon) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(daemon);
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        log.error("Uncaught exception in thread '{}':", t.getName(), e);
      }
    });
    return thread;
  }

  private String legalDisclaimerProactiveSupportEnabled(long reportIntervalHours) {
    return "Please note that the support metrics collection feature (\"Metrics\") of Proactive Support is enabled.  " +
        "With Metrics enabled, this broker is configured to collect and report certain broker and " +
        "cluster metadata (\"Metadata\") about your use of the Confluent Platform (including " +
        "without limitation, your remote internet protocol address) to Confluent, Inc. " +
        "(\"Confluent\") or its parent, subsidiaries, affiliates or service providers every " +
        reportIntervalHours +
        "hours.  This Metadata may be transferred to any country in which Confluent maintains " +
        "facilities.  For a more in depth discussion of how Confluent processes such information, " +
        "please read our Privacy Policy located at http://www.confluent.io/privacy. " +
        "By proceeding with `" + SupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG + "=true`, " +
        "you agree to all such collection, transfer, storage and use of Metadata by Confluent.  " +
        "You can turn the Metrics feature off by setting `" +
        SupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG + "=false` in the broker " +
        "configuration and restarting the broker.  See the Confluent Platform documentation for " +
        "further information.";
  }

  private String legalDisclaimerProactiveSupportDisabled() {
    return "The support metrics collection feature (\"Metrics\") of Proactive Support is disabled.";
  }

  public void startup() {
    try {
      server.startup();
    } catch (Exception e) {
      System.exit(ExitCodes.ERROR);
    }

    try {
      if (metricsThread != null) {
        metricsThread.start();
      }
    } catch (Exception e) {
      // We catch any exceptions to prevent collateral damage to the more important broker
      // threads that are running in the same JVM.
      log.error("Failed to start metrics collection thread: {}", e.getMessage());
    }
  }

  public void shutdown() {
    try {
      if (metricsThread != null) {
        metricsThread.interrupt();
        metricsThread.join();
      }
    } catch (Exception e) {
      // We catch any exceptions to prevent collateral damage to the more important broker
      // threads that are running in the same JVM.
      log.error("Failed to shut down metrics collection thread: {}", e.getMessage());
    }

    try {
      server.shutdown();
    } catch (Exception e) {
      // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
      Runtime.getRuntime().halt(ExitCodes.ERROR);
    }
  }

  /**
   * Allow setting broker state from the startable. This is needed when a custom kafka server
   * startable want to emit new states that it introduces.
   */
  public void setServerState(Byte newState) {
    server.brokerState().newState(newState);
  }

  public void awaitShutdown() {
    server.awaitShutdown();
  }

  /**
   * This method is protected for unit testing
   */
  protected final MetricsReporter getMetricsReporter() {
    return metricsReporter;
  }

  /**
   * This method is protected for unit testing
   */
  protected final boolean isProactiveSupportActiveAtRuntime() {
    return getMetricsReporter() != null;
  }

}
