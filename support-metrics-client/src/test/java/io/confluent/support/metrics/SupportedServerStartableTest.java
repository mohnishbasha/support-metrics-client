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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SupportedServerStartableTest {
  private static final Logger log = LoggerFactory.getLogger(SupportedServerStartableTest.class);
  private static Properties supportProperties = null;

  static {
    try {
      Properties props = new Properties();
      props.load(MetricsToKafkaTest.class.getResourceAsStream("/default-server.properties"));
      supportProperties = props;
    } catch (IOException e) {
      log.warn("Error while loading default properties:", e.getMessage());
    }
  }

  @Test
  public void testProactiveSupportEnabled() {
      Properties props = (Properties)supportProperties.clone();
      SupportedServerStartable supportedServerStartable = new SupportedServerStartable(props);

      assertThat(supportedServerStartable.getMetricsReporter()).isNotNull();
      assertThat(supportedServerStartable.getMetricsReporter().reportingEnabled()).isTrue();
      assertThat(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled()).isTrue();
      assertThat(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled()).isTrue();
  }

  @Test
  public void testProactiveSupportDisabled() {
    Properties props = (Properties)supportProperties.clone();
    props.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(props);

    assertThat(supportedServerStartable.getMetricsReporter()).isNull();
  }

  @Test
  public void testProactiveSupportEnabledKafkaOnly() {
    Properties props = (Properties)supportProperties.clone();
    props.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "");
    props.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(props);

    assertThat(supportedServerStartable.getMetricsReporter().reportingEnabled()).isTrue();
    assertThat(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled()).isFalse();
    assertThat(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled()).isTrue();
  }

  @Test
  public void testProactiveSupportEnabledConfluentOnly() {
    Properties props = (Properties)supportProperties.clone();
    props.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(props);

    assertThat(supportedServerStartable.getMetricsReporter().reportingEnabled()).isTrue();
    assertThat(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled()).isTrue();
    assertThat(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled()).isFalse();
  }

}
