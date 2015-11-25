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
package io.confluent.support.metrics;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import kafka.metrics.KafkaMetricsReporter$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.VerifiableProperties;
import scala.Option;

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

  public SupportedServerStartable(Properties props) {
    KafkaMetricsReporter$.MODULE$.startReporters(new VerifiableProperties(props));
    KafkaConfig serverConfig = KafkaConfig.fromProps(props);
    Option<String> noThreadNamePrefix = Option.empty();
    server = new KafkaServer(serverConfig, SystemTime$.MODULE$, noThreadNamePrefix);

    try {
      if (SupportConfig.isProactiveSupportEnabled(props)) {
        Runtime serverRuntime = Runtime.getRuntime();
        metricsReporter = new MetricsReporter(server, props, serverRuntime);
        metricsThread = Utils.daemonThread("BrokerMetricsReporter", metricsReporter);
      } else {
        log.warn("Proactive Support is disabled");
      }
    } catch (Exception e) {
      // We catch any exceptions to prevent collateral damage to the more important broker
      // threads that are running in the same JVM.
      log.error("Failed to start metrics reporter: {}", e.getMessage());
    }
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

}