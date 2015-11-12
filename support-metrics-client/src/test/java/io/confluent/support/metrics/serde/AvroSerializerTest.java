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

package io.confluent.support.metrics.serde;

import org.apache.avro.generic.GenericContainer;
import org.junit.Test;


import java.util.List;
import io.confluent.support.metrics.collectors.BasicCollector;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.TimeUtils;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class AvroSerializerTest {
    private final TimeUtils time = new TimeUtils();
    private final AvroSerializer encoder = new AvroSerializer();
    private final AvroDeserializer decoder = new AvroDeserializer();

    @Test
    public void testBasicCollectorSerializeDeserializeMatchSchema() {
        byte[] encodedMetricsRecord = null;
        Collector metricsCollector = new BasicCollector(time);
        GenericContainer metricsRecord = metricsCollector.collectMetrics();
        GenericContainer[] metricsRecord2 = null;

        // serialize
        try {
            encodedMetricsRecord = encoder.serialize(metricsRecord);
        } catch (Exception e) {
            fail("Success expected because serialize values are valid");
        }

        // deserialize
        try {
            metricsRecord2 = decoder.deserialize(metricsRecord.getSchema(), encodedMetricsRecord);
        } catch (Exception e) {
            fail("Success expected because deserialize values are valid");
        }
        assertThat(metricsRecord2.length).isEqualTo(1);
        assertThat(metricsRecord.getSchema()).isEqualTo(metricsRecord2[0].getSchema());
    }

}
