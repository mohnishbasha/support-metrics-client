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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SupportConfigTest {

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
  public void testValidCustomer() {
    for (String validId : validCustomerIds) {
      assertThat(validId + " is an invalid customer identifier",
          SupportConfig.isConfluentCustomer(validId), is(true));
    }
  }

  @Test
  public void testInvalidCustomer() {
    String[] invalidIds = ObjectArrays.concat(invalidCustomerIds, validAnonymousIds, String.class);
    for (String invalidCustomerId : invalidIds) {
      assertThat(invalidCustomerId + " is a valid customer identifier",
          SupportConfig.isConfluentCustomer(invalidCustomerId), is(false));
    }
  }

  @Test
  public void testValidAnonymousUser() {
    for (String validId : validAnonymousIds) {
      assertThat(validId + " is an invalid anonymous user identifier",
          SupportConfig.isAnonymousUser(validId), is(true));
    }
  }

  @Test
  public void testInvalidAnonymousUser() {
    String[] invalidIds = ObjectArrays.concat(invalidAnonymousIds, validCustomerIds, String.class);
    for (String invalidId : invalidIds) {
      assertThat(invalidId + " is a valid anonymous user identifier",
          SupportConfig.isAnonymousUser(invalidId), is(false));
    }
  }

  @Test
  public void testCustomerIdValidSettings() {
    String[] validValues = ObjectArrays.concat(validAnonymousIds, validCustomerIds, String.class);
    for (String validValue : validValues) {
      assertThat(validValue + " is an invalid value for " + SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          SupportConfig.isSyntacticallyCorrectCustomerId(validValue), is(true));
    }
  }

  @Test
  public void testCustomerIdInvalidSettings() {
    String[] invalidValues = ObjectArrays.concat(invalidAnonymousIds, invalidCustomerIds, String.class);
    for (String invalidValue : invalidValues) {
      assertThat(invalidValue + " is a valid value for " + SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          SupportConfig.isSyntacticallyCorrectCustomerId(invalidValue), is(false));
    }
  }

}