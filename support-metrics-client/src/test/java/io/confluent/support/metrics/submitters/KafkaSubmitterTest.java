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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class KafkaSubmitterTest {

  @Test
  public void testInvalidArgumentsForConstructorNullBootstrapServers() {
    // Given
    String nullBootstrapServers = null;
    String anyTopic = "valueNotRelevant";

    // When/Then
    try {
      new KafkaSubmitter(nullBootstrapServers, anyTopic);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify bootstrap servers");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyBootstrapServers() {
    // Given
    String emptyBootstrapServers = "";
    String anyTopic = "valueNotRelevant";

    // When/Then
    try {
      new KafkaSubmitter(emptyBootstrapServers, anyTopic);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify bootstrap servers");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullTopic() {
    // Given
    String anyBootstrapServers = "valueNotRelevant";
    String nullTopic = null;

    // When/Then
    try {
      new KafkaSubmitter(anyBootstrapServers, nullTopic);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify topic");
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyTopic() {
    // Given
    String anyBootstrapServers = "valueNotRelevant";
    String emptyTopic = "";

    // When/Then
    try {
      new KafkaSubmitter(anyBootstrapServers, emptyTopic);
      fail("IllegalArgumentException expected because topic is the empty string");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessage("must specify topic");
    }
  }

}