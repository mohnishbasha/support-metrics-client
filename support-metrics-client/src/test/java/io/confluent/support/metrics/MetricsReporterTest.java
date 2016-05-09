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

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsReporterTest {

  private static KafkaServer mockServer;

  @BeforeClass
  public static void startCluster() {
    ZkUtils mockZkUtils = mock(ZkUtils.class);
    KafkaConfig mockConfig = mock(KafkaConfig.class);
    when(mockConfig.advertisedHostName()).thenReturn("anyHostname");
    when(mockConfig.advertisedPort()).thenReturn(12345);
    mockServer = mock(KafkaServer.class);
    when(mockServer.zkUtils()).thenReturn(mockZkUtils);
    when(mockServer.config()).thenReturn(mockConfig);
  }

  @Test
  public void testInvalidArgumentsForConstructorNullServer() throws Exception {
    // Given
    Properties emptyProperties = new Properties();
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter(null, emptyProperties, serverRuntime);
      fail("IllegalArgumentException expected because server is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("some arguments are null");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullProperties() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter(mockServer, null, serverRuntime);
      fail("IllegalArgumentException expected because props is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("some arguments are null");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullRuntime() {
    // Given
    Properties emptyProperties = new Properties();

    // When/Then
    try {
      new MetricsReporter(mockServer, emptyProperties, null);
      fail("IllegalArgumentException expected because serverRuntime is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("some arguments are null");
    }
  }


  @Test
  public void testValidConstructorTopicOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    Runtime serverRuntime = Runtime.getRuntime();

    // When
    MetricsReporter reporter = new MetricsReporter(mockServer, serverProperties, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isTrue();
    assertThat(reporter.sendToKafkaEnabled()).isTrue();
    assertThat(reporter.sendToConfluentEnabled()).isFalse();
  }

  @Test
  public void testValidConstructorHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "http://example.com/");
    Runtime serverRuntime = Runtime.getRuntime();

    // When
    MetricsReporter reporter = new MetricsReporter(mockServer, serverProperties, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isTrue();
    assertThat(reporter.sendToKafkaEnabled()).isFalse();
    assertThat(reporter.sendToConfluentEnabled()).isTrue();
  }

  @Test
  public void testValidConstructorHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "https://example.com/");
    Runtime serverRuntime = Runtime.getRuntime();

    // When
    MetricsReporter reporter = new MetricsReporter(mockServer, serverProperties, serverRuntime);

    // Then
    assertThat(reporter.reportingEnabled()).isTrue();
    assertThat(reporter.sendToKafkaEnabled()).isFalse();
    assertThat(reporter.sendToConfluentEnabled()).isTrue();
  }

  @Test
  public void testInvalidConstructorInvalidHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "http://example.com/");
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter(mockServer, serverProperties, serverRuntime);
      fail("IllegalArgumentException expected because secure endpoint was of wrong type");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageStartingWith("invalid HTTPS endpoint");
    }
  }

  @Test
  public void testInvalidConstructorInvalidHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "https://example.com/");
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter(mockServer, serverProperties, serverRuntime);
      fail("IllegalArgumentException expected because insecure endpoint was of wrong type");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageStartingWith("invalid HTTP endpoint");
    }
  }
}