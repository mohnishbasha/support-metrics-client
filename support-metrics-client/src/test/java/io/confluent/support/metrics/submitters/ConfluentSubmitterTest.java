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

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;

import io.confluent.support.metrics.SupportConfig;
import io.confluent.support.metrics.SupportConfigTest;
import io.confluent.support.metrics.utils.cIdUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class ConfluentSubmitterTest {

  private String customerId = SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;

  private final String[] validCustomerIds = {
      "C0", "c1", "C1", "c12", "C22", "c123", "C333", "c1234", "C4444",
      "C00000", "C12345", "C99999", "C123456789", "C123456789012345678901234567890",
      "c00000", "c12345", "c99999", "c123456789", "c123456789012345678901234567890",
  };

  // These invalid customer ids should not include valid anonymous user IDs.
  private final String[] invalidCustomerIds = {
      "0c000", "0000C", null, "", "c", "C", "Hello", "World", "1", "12", "123", "1234", "12345"
  };

  private final String[] validAnonymousIds = {"anonymous", "ANONYMOUS", "anonyMOUS"};

  // These invalid anonymous user IDs should not include valid customer IDs.
  private final String[] invalidAnonymousIds = {null, "", "anon", "anonymou", "ANONYMOU"};


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
    } catch (Exception e) {
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
    } catch (Exception e) {
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
    } catch (Exception e) {
      assertThat(e).hasMessageStartingWith("invalid HTTP endpoint");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorCustomerIdNull() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    try {
      new ConfluentSubmitter(null, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because customer ID is null");
    } catch (Exception e) {
      assertThat(e).hasMessageStartingWith("invalid customer ID");
    }
  }

  @Test
  public void testSubmitIgnoresNullInput() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";
    HttpPost p = mock(HttpPost.class);
    ConfluentSubmitter c = new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
    byte[] nullData = null;

    // When
    c.send(nullData, p);

    // Then
    verifyZeroInteractions(p);
  }

  @Test
  public void testSubmitIgnoresEmptyInput() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";
    HttpPost p = mock(HttpPost.class);
    ConfluentSubmitter c = new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
    byte[] emptyData = new byte[0];

    // When
    c.send(emptyData, p);

    // Then
    verifyZeroInteractions(p);
  }

  @Test
  public void testSubmitInvalidCustomer() {
    // Given
    String httpEndpoint = cIdUtils.testHttpEndpoint;
    HttpPost p = new HttpPost(httpEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String invalidCustomerId : cIdUtils.invalidCustomerIds) {
      System.out.println("CUSTOMER:" + invalidCustomerId);
      try {
        ConfluentSubmitter c = new ConfluentSubmitter(invalidCustomerId, httpEndpoint, null);
        assertThat(c.send(anyData, p)).isNotEqualTo(HttpStatus.SC_OK);
      } catch (Exception e){
        assertThat(e).hasMessageStartingWith("invalid customer ID");
      }
    }
  }

  @Test
  public void testSubmitInvalidAnonymousUser() {
    // Given
    String httpEndpoint = cIdUtils.testHttpEndpoint;
    HttpPost p = new HttpPost(httpEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String invalidCustomerId : cIdUtils.invalidAnonymousIds) {
      try {
        ConfluentSubmitter c = new ConfluentSubmitter(invalidCustomerId, httpEndpoint, null);
        assertThat(c.send(anyData, p)).isNotEqualTo(HttpStatus.SC_OK);
      } catch (Exception e){
        assertThat(e).hasMessageStartingWith("invalid customer ID");
      }

    }
  }

  @Test
  public void testSubmitValidCustomer() {
    // Given
    String httpEndpoint = cIdUtils.testHttpEndpoint;
    HttpPost p = new HttpPost(httpEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String invalidCustomerId : cIdUtils.validCustomerIds) {
      ConfluentSubmitter c = new ConfluentSubmitter(invalidCustomerId, httpEndpoint, null);
      int status = c.send(anyData, p);
      // if we are not connected to the internet this test should still pass
      assertThat(status == HttpStatus.SC_OK || status == HttpStatus.SC_BAD_GATEWAY).isTrue();
    }
  }

  @Test
  public void testSubmitValidAnonymousUser() {
    // Given
    String httpEndpoint = cIdUtils.testHttpEndpoint;
    HttpPost p = new HttpPost(httpEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    for (String invalidCustomerId : cIdUtils.validAnonymousIds) {
      ConfluentSubmitter c = new ConfluentSubmitter(invalidCustomerId, httpEndpoint, null);
      int status = c.send(anyData, p);
      // if we are not connected to the internet this test should still pass
      assertThat(status == HttpStatus.SC_OK || status == HttpStatus.SC_BAD_GATEWAY).isTrue();
    }
  }
}