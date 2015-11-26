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

public class cIdUtils {

  public static final String testHttpEndpoint = "http://support-metrics.confluent.io/test";
  public static final String[] validCustomerIds = {
      "C0", "c1", "C1", "c12", "C22", "c123", "C333", "c1234", "C4444",
      "C00000", "C12345", "C99999", "C123456789", "C123456789012345678901234567890",
      "c00000", "c12345", "c99999", "c123456789", "c123456789012345678901234567890",
  };

  // These invalid customer ids should not include valid anonymous user IDs.
  public static final String[] invalidCustomerIds = {
      "0c000", "0000C", null, "", "c", "C", "Hello", "World", "1", "12", "123", "1234", "12345"
  };

  public static final String[] validAnonymousIds = {"anonymous", "ANONYMOUS", "anonyMOUS"};

  // These invalid anonymous user IDs should not include valid customer IDs.
  public static final String[] invalidAnonymousIds = {null, "", "anon", "anonymou", "ANONYMOU"};

}
