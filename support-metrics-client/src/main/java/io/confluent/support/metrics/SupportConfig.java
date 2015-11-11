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

// TODO: Document these settings.

/**
 * Configuration for the Confluent Support options. Documentation for these configurations can be
 * found in TBD
 */
public class SupportConfig {

  /**
   * <code>confluent.support.customer.id</code>
   */
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG = "confluent.support.customer.id";
  private static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DOC = "Customer ID assigned by Confluent";
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT = "anonymous";

  public static boolean isAnonymousCustomerId(String customerId) {
    return customerId.equals(CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
  }

  /**
   * <code>confluent.support.report.interval.hours</code>
   */
  public static final String CONFLUENT_SUPPORT_REPORT_INTERVAL_HOURS_CONFIG = "confluent.support.report.interval.hours";
  private static final String CONFLUENT_SUPPORT_REPORT_INTERVAL_HOURS_DOC = "Frequency of reporting in hours, e.g., 24 would indicate every day ";
  public static final String CONFLUENT_SUPPORT_REPORT_INTERVAL_HOURS_DEFAULT = "24";

  /**
   * <code>confluent.support.internal.kafka.topic</code>
   */
  public static final String CONFLUENT_SUPPORT_INTERNAL_KAFKA_TOPIC_CONFIG = "confluent.support.internal.kafka.topic";
  private static final String CONFLUENT_SUPPORT_INTERNAL_KAFKA_TOPIC_DOC = "Internal topic used for metric collection. If missing, metrics will not be collected in a Kafka topic ";

  /**
   * <code>confluent.support.confluent.http</code>
   */
  public static final String CONFLUENT_SUPPORT_CONFLUENT_HTTP_CONFIG = "confluent.support.confluent.http";
  private static final String CONFLUENT_SUPPORT_CONFLUENT_HTTP_DOC = "Confluent endpoint that receives metrics over HTTP";

  /**
   * <code>confluent.support.confluent.https</code>
   */
  public static final String CONFLUENT_SUPPORT_CONFLUENT_HTTPS_CONFIG = "confluent.support.confluent.https";
  private static final String CONFLUENT_SUPPORT_CONFLUENT_HTTPS_DOC = "Confluent endpoint that receives metrics over HTTPS";
}
