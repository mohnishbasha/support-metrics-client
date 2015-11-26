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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import io.confluent.support.metrics.utils.KafkaServerUtils;
import kafka.Kafka;
import kafka.server.KafkaServer;
import kafka.zk.EmbeddedZookeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class MetricsReporterTest {

  private static EmbeddedZookeeper zookeeper = null;
  private static KafkaServer server = null;

  @BeforeClass
  public static void startCluster() {
    zookeeper = KafkaServerUtils.startZookeeper();
    server = KafkaServerUtils.startServer(zookeeper);
  }

  @AfterClass
  public static void stopCluster() {
    KafkaServerUtils.stopServer(server);
    KafkaServerUtils.stopZookeeper(zookeeper);
  }

  @Test
  public void testInvalidArgumentsForConstructorNullServer() {
    // Given
    Properties props = new Properties();
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter(null, props, serverRuntime);
      fail("IllegalArgumentException expected because server is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("some arguments are null");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullProps() {
    // Given
    Properties nullProperties = null;
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter(server, nullProperties, serverRuntime);
      fail("IllegalArgumentException expected because props is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("some arguments are null");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullRuntime() {
    // Given
    Properties emptyProperties = new Properties();
    Runtime nullRuntime = null;

    // When/Then
    try {
      new MetricsReporter(server, emptyProperties, nullRuntime);
      fail("IllegalArgumentException expected because serverRuntime is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("some arguments are null");
    }
  }

  @Test
  public void testValidConstructor() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});

    // When
    MetricsReporter reporter = new MetricsReporter(server, serverProps, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isEqualTo(true);
    assertThat(reporter.sendToKafkaEnabled()).isEqualTo(true);
    assertThat(reporter.sendToConfluentEnabled()).isEqualTo(true);
  }

  @Test
  public void testValidConstructorTopicOnly() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);

    // When
    MetricsReporter reporter = new MetricsReporter(server, serverProps, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isEqualTo(true);
    assertThat(reporter.sendToKafkaEnabled()).isEqualTo(true);
    assertThat(reporter.sendToConfluentEnabled()).isEqualTo(false);
  }

  @Test
  public void testValidConstructorHTTPOnly() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);

    // When
    MetricsReporter reporter = new MetricsReporter(server, serverProps, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isEqualTo(true);
    assertThat(reporter.sendToKafkaEnabled()).isEqualTo(false);
    assertThat(reporter.sendToConfluentEnabled()).isEqualTo(true);
  }

  @Test
  public void testValidConstructorHTTPSOnly() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);

    // When
    MetricsReporter reporter = new MetricsReporter(server, serverProps, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isEqualTo(true);
    assertThat(reporter.sendToKafkaEnabled()).isEqualTo(false);
    assertThat(reporter.sendToConfluentEnabled()).isEqualTo(true);
  }

  @Test
  public void testValidConstructorInvalidHTTPSOnly() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "http://example.com");

    // When/Then
    try {
      new MetricsReporter(server, serverProps, serverRuntime);
      fail("IllegalArgumentException expected because endpoints was of wrong type");
    } catch (Exception e) {
      assertThat(e).hasMessageStartingWith("invalid HTTPS endpoint");
    }
  }

  @Test
  public void testValidConstructorInvalidHTTPOnly() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);
    serverProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "https://example.com");

    // When/Then
    try {
      new MetricsReporter(server, serverProps, serverRuntime);
      fail("IllegalArgumentException expected because endpoints was of wrong type");
    } catch (Exception e) {
      assertThat(e).hasMessageStartingWith("invalid HTTP endpoint");
    }
  }
}