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
package io.confluent.support.metrics.submitters;


import org.junit.Test;


import io.confluent.support.metrics.SupportConfig;
import io.confluent.support.metrics.utils.CustomerIdExamples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ConfluentSubmitterTest {

  private String customerId = SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;


  @Test
  public void testInvalidArgumentsForConstructorNullEndpoints() {
    // Given
    String httpEndpoint = null;
    String httpsEndpoint = null;

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify endpoints");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyEndpoints() {
    // Given
    String httpEndpoint = "";
    String httpsEndpoint = "";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are empty");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify endpoints");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullEmptyEndpoints() {
    // Given
    String httpEndpoint = null;
    String httpsEndpoint = "";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are null/empty");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify endpoints");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyNullEndpoints() {
    // Given
    String httpEndpoint = "";
    String httpsEndpoint = null;

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are empty/null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify endpoints");
    }
  }

  @Test
  public void testValidArgumentsForConstructor() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
  }

  @Test
  public void testInvalidArgumentsForConstructorInvalidHttpEndpoint() {
    // Given
    String httpEndpoint = "invalid URL";
    String httpsEndpoint = "https://example.com";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are invalid");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageStartingWith("invalid HTTP endpoint");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorInvalidHttpsEndpoint() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "invalid URL";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are invalid");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageStartingWith("invalid HTTPS endpoint");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorMismatchedEndpoints() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpsEndpoint, httpEndpoint);
      fail("IllegalArgumentException expected because endpoints were provided in the wrong order");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageStartingWith("invalid HTTP endpoint");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorCustomerIdInvalid() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    for (String invalidCustomerId : CustomerIdExamples.invalidCustomerIds) {
      try {
        new ConfluentSubmitter(invalidCustomerId, httpEndpoint, httpsEndpoint);
        fail("IllegalArgumentException expected because customer ID is invalid");
      } catch (IllegalArgumentException e) {
        assertThat(e).hasMessageStartingWith("invalid customer ID");
      }
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorAnonymousIdInvalid() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    for (String invalidCustomerId : CustomerIdExamples.invalidAnonymousIds) {
      try {
        new ConfluentSubmitter(invalidCustomerId, httpEndpoint, httpsEndpoint);
        fail("IllegalArgumentException expected because customer ID is invalid");
      } catch (IllegalArgumentException e) {
        assertThat(e).hasMessageStartingWith("invalid customer ID");
      }
    }
  }
}