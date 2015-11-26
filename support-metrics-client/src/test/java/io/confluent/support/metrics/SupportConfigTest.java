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

import java.util.Properties;

import io.confluent.support.metrics.utils.KafkaServerUtils;
import io.confluent.support.metrics.utils.CustomerIdExamples;
import kafka.Kafka;
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
  public void proactiveSupportConfigIsValidKafkaConfig() {
    String filePath = KafkaServerUtils.pathToDefaultBrokerConfiguration();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{filePath});
    KafkaConfig cfg = KafkaConfig.fromProps(serverProps);

    assertThat(cfg.brokerId()).isEqualTo(1);
    assertThat(cfg.zkConnect()).isEqualTo("localhost:2181");
  }

  @Test
  public void canParseProactiveSupportConfiguration() {
    // Given
    String filePath = KafkaServerUtils.pathToDefaultBrokerConfiguration();
    Properties serverProps = Kafka.getPropsFromArgs(new String[]{filePath});

    // When/Then
    assertThat(SupportConfig.getCustomerId(serverProps)).isEqualTo("anonymous");
    assertThat(SupportConfig.getReportIntervalMs(serverProps)).isEqualTo(24 * 60 * 60 * 1000);
    assertThat(SupportConfig.getKafkaTopic(serverProps)).isEqualTo("__sample_topic");
    assertThat(SupportConfig.getEndpointHTTP(serverProps)).isEqualTo("http://support-metrics.confluent.io/test");
    assertThat(SupportConfig.getEndpointHTTPS(serverProps)).isEqualTo("https://support-metrics.confluent.io/test");
    assertThat(SupportConfig.isProactiveSupportEnabled(serverProps)).isTrue();

  }

  @Test
  public void isProactiveSupportEnabledFull() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "http://example.com");
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "https://example.com");

    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isTrue();
  }

  @Test
  public void isProactiveSupportEnabledTopicOnly() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");

    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isTrue();
  }

  @Test
  public void isProactiveSupportEnabledHTTPOnly() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "http://example.com");

    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isTrue();
  }

  @Test
  public void isProactiveSupportEnabledHTTPSOnly() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "https://example.com");

    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isTrue();
  }

  @Test
  public void isProactiveSupportDisabled() {
    Properties serverProperties = new Properties();

    assertThat(SupportConfig.isProactiveSupportEnabled(serverProperties)).isFalse();
  }

}