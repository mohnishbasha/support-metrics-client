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

import junit.framework.AssertionFailedError;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.*;

public class ConfluentSubmitterTest {

    @Test
    public void testInvalidArgumentsForConstructorNullEndpoints() {
        // Given
        String httpEndpoint = null;
        String httpsEndpoint = null;

        // When/Then
        try {
            new ConfluentSubmitter(httpEndpoint, httpsEndpoint);
            fail("IllegalArgumentException expected because endpoints are null");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("must specify Confluent Service endpoint");
        }
    }

    @Test
    public void testInvalidArgumentsForConstructorEmptyEndpoints() {
        // Given
        String httpEndpoint = "";
        String httpsEndpoint = "";

        // When/Then
        try {
            new ConfluentSubmitter(httpEndpoint, httpsEndpoint);
            fail("IllegalArgumentException expected because endpoints are empty");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("must specify Confluent Service endpoint");
        }
    }

    @Test
    public void testInvalidArgumentsForConstructorNullEmptyEndpoints() {
        // Given
        String httpEndpoint = null;
        String httpsEndpoint = "";

        // When/Then
        try {
            new ConfluentSubmitter(httpEndpoint, httpsEndpoint);
            fail("IllegalArgumentException expected because endpoints are empty");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("must specify Confluent Service endpoint");
        }
    }

    @Test
    public void testInvalidArgumentsForConstructorEmptyNullEndpoints() {
        // Given
        String httpEndpoint = "";
        String httpsEndpoint = null;

        // When/Then
        try {
            new ConfluentSubmitter(httpEndpoint, httpsEndpoint);
            fail("IllegalArgumentException expected because endpoints are empty");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("must specify Confluent Service endpoint");
        }
    }

    @Test
    public void testValidArgumentsForConstructor() {
        // Given
        String httpEndpoint = "http://endpoint1";
        String httpsEndpoint = "https://endpoint1";

        // When/Then
        try {
            new ConfluentSubmitter(httpEndpoint, httpsEndpoint);
        } catch (Exception e) {
            fail("Success expected because endpoints are valid");
        }
    }

    @Test
    public void testSubmitIgnoresNullInput() {
        // Given
        String httpEndpoint = "valueNotRelevant";
        String httpsEndpoint = "valueNotRelevant";

        // When
        ConfluentSubmitter c = new ConfluentSubmitter(httpEndpoint, httpsEndpoint);
        byte[] nullData = null;

        // When
        try {
            c.submit(nullData);
            fail("IllegalArgumentException expected because data is NULL");
        } catch (Exception e) {
            assertThat(e).hasMessage("must send non-NULL record");
        }
    }
}