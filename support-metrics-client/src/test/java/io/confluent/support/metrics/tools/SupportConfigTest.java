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
    String[] idWellFormed = {"C10239", "C00000", "c43345", "anonymous", "ANONYmouS"};

    for (String s : idWellFormed) {
      if (!SupportConfig.isWellFormedCustomerId(s)) {
        fail("String expected to be well-formed customer ID");
      }
    }
  }

  @Test
  public void testNotWellFormedCustomerId() {
    // Given
    String[] idNotWellFormed = {"", "C", "C1", "C12", "123", "C123456", "1234567"};

    for (String s : idNotWellFormed) {
      if (SupportConfig.isWellFormedCustomerId(s)) {
        fail("String is NOT expected to be well-formed customer ID");
      }
    }
  }

}