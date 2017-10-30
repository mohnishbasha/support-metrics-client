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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * Configuration for the Confluent Support options.
 *
 * Note: These Confluent-specific settings are added to {@code config/server.properties} by
 * Confluent's <a href="https://github.com/confluentinc/kafka-packaging">kafka-packaging</a> via a
 * patch file.  If you need to make any changes (e.g. renaming settings, adding/removing settings),
 * then make sure to also update the patch file accordingly.
 */
public class KafkaSupportConfig extends BaseSupportConfig {
  private static final String PROPRIETARY_PACKAGE_NAME = "io.confluent.support.metrics.collectors.FullCollector";
  private static final Logger log = LoggerFactory.getLogger(KafkaSupportConfig.class);

  public KafkaSupportConfig(Properties originals) {

    super(setupProperties(originals));

  }

  private static Properties setupProperties(Properties originals){
    try {
      Class.forName(PROPRIETARY_PACKAGE_NAME);
    } catch(ClassNotFoundException e) {
      originals.setProperty(
          KafkaSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          KafkaSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
      log.warn(warningIfFullCollectorPackageMissing());
    }
    originals.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_COMPONENT_CONFIG,"kafka");
    return originals;
  }

  private static String warningIfFullCollectorPackageMissing() {
    return "The package " +  PROPRIETARY_PACKAGE_NAME + " for collecting the full set of support metrics " +
        "could not be loaded, so we are reverting to anonymous, basic metric collection. " +
        "If you are a Confluent customer, please refer to the Confluent Platform documentation, " +
        "section Proactive Support, on how to activate full metrics collection.";
  }

}
