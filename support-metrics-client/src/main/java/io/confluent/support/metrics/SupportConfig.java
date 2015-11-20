/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.support.metrics;

import java.util.regex.*;

// TODO: Document these settings.

/**
 * Configuration for the Confluent Support options. Documentation for these configurations can be
 * found in TBD
 *
 * Note: These Confluent-specific settings are added to {@code config/server.properties} by
 * Confluent's <a href="https://github.com/confluentinc/kafka-packaging">kafka-packaging</a> via a
 * patch file.  If you need to make any changes (e.g. renaming settings, adding/removing settings),
 * then make sure to also update the patch file accordingly.
 */
public class SupportConfig {

  /**
   * <code>confluent.support.customer.id</code>
   */
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG = "confluent.support.customer.id";
  private static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DOC = "Customer ID assigned by Confluent";
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT = "anonymous";
  private static final String customerPattern = "c\\d{5}";

  public static boolean isAnonymousCustomerId(String customerId) {
    return customerId.toLowerCase().equals(CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
  }

  public static boolean isWellFormedCustomerId(String customerId) {
    if (isAnonymousCustomerId(customerId)) {
      return true;
    }

    Pattern pattern = Pattern.compile(customerPattern);
    Matcher matcher = pattern.matcher(customerId.toLowerCase());
    return matcher.matches();
  }

  /**
   * <code>confluent.support.metrics.report.interval.hours</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG = "confluent.support.metrics.report.interval.hours";
  private static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DOC = "Frequency of reporting in hours, e.g., 24 would indicate every day ";
  public static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT = "24";

  /**
   * <code>confluent.support.metrics.topic</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG = "confluent.support.metrics.topic";
  private static final String CONFLUENT_SUPPORT_METRICS_TOPIC_DOC = "Internal topic used for metric collection. If missing, metrics will not be collected in a Kafka topic ";

  /**
   * <code>confluent.support.metrics.endpoint.insecure</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG = "confluent.support.metrics.endpoint.insecure";
  private static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DOC = "Confluent endpoint that receives metrics over HTTP";

  /**
   * <code>confluent.support.metrics.endpoint.secure</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG = "confluent.support.metrics.endpoint.secure";
  private static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DOC = "Confluent endpoint that receives metrics over HTTPS";
}
