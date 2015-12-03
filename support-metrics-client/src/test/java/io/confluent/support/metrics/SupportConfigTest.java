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

import com.google.common.collect.ObjectArrays;

import org.junit.Test;


import java.io.IOException;
import java.util.Properties;

import io.confluent.support.metrics.utils.CustomerIdExamples;
import kafka.server.KafkaConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SupportConfigTest {

  @Test
  public void testValidCustomer() {
    for (String validId : CustomerIdExamples.validCustomerIds) {
      assertThat(validId + " is an invalid customer identifier",
          SupportConfig.isConfluentCustomer(validId), is(true));
    }
  }

  @Test
  public void testInvalidCustomer() {
    String[] invalidIds = ObjectArrays.concat(CustomerIdExamples.invalidCustomerIds, CustomerIdExamples.validAnonymousIds, String.class);
    for (String invalidCustomerId : invalidIds) {
      assertThat(invalidCustomerId + " is a valid customer identifier",
          SupportConfig.isConfluentCustomer(invalidCustomerId), is(false));
    }
  }

  @Test
  public void testValidAnonymousUser() {
    for (String validId : CustomerIdExamples.validAnonymousIds) {
      assertThat(validId + " is an invalid anonymous user identifier",
          SupportConfig.isAnonymousUser(validId), is(true));
    }
  }

  @Test
  public void testInvalidAnonymousUser() {
    String[] invalidIds = ObjectArrays.concat(CustomerIdExamples.invalidAnonymousIds, CustomerIdExamples.validCustomerIds, String.class);
    for (String invalidId : invalidIds) {
      assertThat(invalidId + " is a valid anonymous user identifier",
          SupportConfig.isAnonymousUser(invalidId), is(false));
    }
  }

  @Test
  public void testCustomerIdValidSettings() {
    String[] validValues = ObjectArrays.concat(CustomerIdExamples.validAnonymousIds, CustomerIdExamples.validCustomerIds, String.class);
    for (String validValue : validValues) {
      assertThat(validValue + " is an invalid value for " + SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          SupportConfig.isSyntacticallyCorrectCustomerId(validValue), is(true));
    }
  }

  @Test
  public void testCustomerIdInvalidSettings() {
    String[] invalidValues = ObjectArrays.concat(CustomerIdExamples.invalidAnonymousIds, CustomerIdExamples.invalidCustomerIds, String.class);
    for (String invalidValue : invalidValues) {
      assertThat(invalidValue + " is a valid value for " + SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          SupportConfig.isSyntacticallyCorrectCustomerId(invalidValue), is(false));
    }
  }


  @Test
  public void proactiveSupportConfigIsValidKafkaConfig() throws IOException {
    // Given
    Properties brokerConfiguration = defaultBrokerConfiguration();

    // When
    KafkaConfig cfg = KafkaConfig.fromProps(brokerConfiguration);

    // Then
    assertThat(cfg.brokerId()).isEqualTo(0);
    assertThat(cfg.zkConnect()).startsWith("localhost:");
  }

  private Properties defaultBrokerConfiguration() throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(SupportedServerStartableTest.class.getResourceAsStream("/default-server.properties"));
    return brokerConfiguration;
  }

  @Test
  public void canParseProactiveSupportConfiguration() throws IOException {
    // Given
    Properties brokerConfiguration = defaultBrokerConfiguration();

    // When/Then
    assertThat(SupportConfig.getMetricsEnabled(brokerConfiguration)).isEqualTo(true);
    assertThat(SupportConfig.getCustomerId(brokerConfiguration)).isEqualTo("c0");
    assertThat(SupportConfig.isProactiveSupportEnabled(brokerConfiguration)).isTrue();

  }

  @Test
  public void testGetDefaultProps() {
    Properties props = SupportConfig.getDefaultProps();
    assertThat(SupportConfig.getMetricsEnabled(props)).isEqualTo(true);
    assertThat(SupportConfig.getCustomerId(props)).isEqualTo("anonymous");
    assertThat(SupportConfig.getReportIntervalMs(props)).isEqualTo(24 * 60 * 60 * 1000);
    assertThat(SupportConfig.getKafkaTopic(props)).isEqualTo("__confluent.support.metrics");
    assertThat(SupportConfig.getEndpointHTTPEnabled(props)).isEqualTo(true);
    assertThat(SupportConfig.getEndpointHTTPSEnabled(props)).isEqualTo(true);
    assertThat(SupportConfig.isProactiveSupportEnabled(props)).isTrue();
  }


  @Test
  public void testMergeAndValidatePropsOverrideNull() {
    // Given
    Properties nullOverrideProp = null;

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(nullOverrideProp);

    // Then
    assertThat(SupportConfig.getMetricsEnabled(props)).isEqualTo(true);
    assertThat(SupportConfig.getCustomerId(props)).isEqualTo("anonymous");
    assertThat(SupportConfig.getReportIntervalMs(props)).isEqualTo(24 * 60 * 60 * 1000);
    assertThat(SupportConfig.getKafkaTopic(props)).isEqualTo("__confluent.support.metrics");
    assertThat(SupportConfig.getEndpointHTTPEnabled(props)).isEqualTo(true);
    assertThat(SupportConfig.getEndpointHTTPSEnabled(props)).isEqualTo(true);
    assertThat(SupportConfig.getEndpointHTTP(props)).isEqualTo("http://support-metrics.confluent.io/anon");
    assertThat(SupportConfig.getEndpointHTTPS(props)).isEqualTo("https://support-metrics.confluent.io/anon");
    assertThat(SupportConfig.isProactiveSupportEnabled(props)).isTrue();
  }

  @Test
  public void testMergeAndValidatePropsNoOverride() {
    // Given
    Properties overrideProps = new Properties();

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getEndpointHTTP(props)).isEqualTo("http://support-metrics.confluent.io/anon");
    assertThat(SupportConfig.getEndpointHTTPS(props)).isEqualTo("https://support-metrics.confluent.io/anon");
  }

  @Test
  public void testMergeAndValidatePropsFilterDisallowedKeys() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "anyValue");
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "anyValue");

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getEndpointHTTP(props)).isEqualTo("http://support-metrics.confluent.io/anon");
    assertThat(SupportConfig.getEndpointHTTPS(props)).isEqualTo("https://support-metrics.confluent.io/anon");
  }


  @Test
  public void testMergeAndValidatePropsCustomerEndpointMismatch() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "C1");

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getEndpointHTTP(props)).isEqualTo("http://support-metrics.confluent.io/submit");
    assertThat(SupportConfig.getEndpointHTTPS(props)).isEqualTo("https://support-metrics.confluent.io/submit");
  }

  @Test
  public void testMergeAndValidatePropsConfluentTestEndpointMismatch() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "C0");

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getEndpointHTTP(props)).isEqualTo("http://support-metrics.confluent.io/test");
    assertThat(SupportConfig.getEndpointHTTPS(props)).isEqualTo("https://support-metrics.confluent.io/test");
  }

  @Test
  public void testMergeAndValidatePropsDisableEndpoints() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getEndpointHTTP(props)).isEmpty();
    assertThat(SupportConfig.getEndpointHTTPS(props)).isEmpty();
  }

  @Test
  public void testOverrideReportInterval() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG, "1");

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getReportIntervalMs(props)).isEqualTo(1 * 60 * 60 * 1000);
  }

  @Test
  public void testOverrideTopc() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "__another_example_topic");

    // When
    Properties props = SupportConfig.mergeAndValidateWithDefaultProperties(overrideProps);

    // Then
    assertThat(SupportConfig.getKafkaTopic(props)).isEqualTo("__another_example_topic");
  }

  @Test
  public void isProactiveSupportEnabledFull() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "true");

    // When/Then
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isTrue();
  }

  @Test
  public void isProactiveSupportDisabledFull() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "true");
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "true");

    // When/Then
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isFalse();
  }

  @Test
  public void isProactiveSupportEnabledTopicOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");

    // When/Then
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isFalse();
  }

  @Test
  public void isProactiveSupportEnabledHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "true");

    // When/Then
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isFalse();
  }

  @Test
  public void isProactiveSupportEnabledHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "true");

    // When/Then
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isFalse();
  }

  @Test
  public void proactiveSupportIsDisabledByDefaultWhenBrokerConfigurationIsEmpty() {
    // Given
    Properties serverProperties = new Properties();

    // When/Then
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isFalse();
  }

}