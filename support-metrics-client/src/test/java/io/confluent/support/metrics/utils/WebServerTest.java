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
package io.confluent.support.metrics.utils;


import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.exceptions.verification.WantedButNotInvoked;

import java.util.Properties;

import io.confluent.support.metrics.SupportConfig;
import io.confluent.support.metrics.submitters.ConfluentSubmitter;
import kafka.Kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class WebServerTest {
  private String customerId = SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;
  private static Properties serverProps;
  private static String secureLiveTestEndpoint;

  @BeforeClass
  public static void startPrep() {
    serverProps = Kafka.getPropsFromArgs(new String[]{KafkaServerUtils.pathToDefaultBrokerConfiguration()});
    secureLiveTestEndpoint = serverProps.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);
  }

  @Test
  public void testSubmitIgnoresNullInput() {
    // Given
    HttpPost p = mock(HttpPost.class);
    byte[] nullData = null;

    // When
    WebServer.send(customerId, nullData, p);

    // Then
    verifyZeroInteractions(p);
  }

  @Test
  public void testSubmitIgnoresEmptyInput() {
    // Given
    HttpPost p = mock(HttpPost.class);
    byte[] emptyData = new byte[0];

    // When
    WebServer.send(customerId, emptyData, p);

    // Then
    verifyZeroInteractions(p);
  }

  @Test
  public void testSubmitInvalidCustomer() {
    // Given
    HttpPost p = new HttpPost(secureLiveTestEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String invalidCustomerId : CustomerIdExamples.invalidCustomerIds) {
      assertThat(WebServer.send(invalidCustomerId, anyData, p)).isNotEqualTo(HttpStatus.SC_OK);
    }
  }

  @Test
  public void testSubmitInvalidAnonymousUser() {
    // Given
    HttpPost p = new HttpPost(secureLiveTestEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String invalidCustomerId : CustomerIdExamples.invalidAnonymousIds) {
      assertThat(WebServer.send(invalidCustomerId, anyData, p)).isNotEqualTo(HttpStatus.SC_OK);
    }
  }

  @Test
  public void testSubmitValidCustomer() {
    // Given
    HttpPost p = new HttpPost(secureLiveTestEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String validCustomerId : CustomerIdExamples.validCustomerIds) {
      int status = WebServer.send(validCustomerId, anyData, p);
      // if we are not connected to the internet this test should still pass
      assertThat(status == HttpStatus.SC_OK || status == HttpStatus.SC_BAD_GATEWAY).isTrue();
    }
  }

  @Test
  public void testSubmitValidAnonymousUser() {
    // Given
    HttpPost p = new HttpPost(secureLiveTestEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String validCustomerId : CustomerIdExamples.validAnonymousIds) {
       int status = WebServer.send(validCustomerId, anyData, p);
      // if we are not connected to the internet this test should still pass
      assertThat(status == HttpStatus.SC_OK || status == HttpStatus.SC_BAD_GATEWAY).isTrue();
    }
  }
}
