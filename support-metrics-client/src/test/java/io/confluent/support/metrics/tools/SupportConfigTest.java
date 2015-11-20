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
import static org.assertj.core.api.Assertions.fail;

public class SupportConfigTest {

  @Test
  public void testWellFormedCustomerId() {
    // Given
    String[] idWellFormed = {"C1", "C00000", "c43", "anonymous", "ANONYmouS", "c123456789012345678901234567890"};

    for (String s : idWellFormed) {
      if (!SupportConfig.isWellFormedCustomerId(s)) {
        fail("String expected to be well-formed customer ID");
      }
    }
  }

  @Test
  public void testNotWellFormedCustomerId() {
    // Given
    String[] idNotWellFormed = {"", "C", "123", "123C456", "1234567c", "c1234567890123456789012345678901"};

    for (String s : idNotWellFormed) {
      if (SupportConfig.isWellFormedCustomerId(s)) {
        fail("String is NOT expected to be well-formed customer ID");
      }
    }
  }

}