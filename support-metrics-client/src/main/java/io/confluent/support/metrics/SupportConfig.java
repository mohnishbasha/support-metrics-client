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

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.regex.Pattern;

import kafka.server.KafkaServer;

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

  private static final Logger log = LoggerFactory.getLogger(SupportConfig.class);

  /**
   * <code>confluent.support.customer.id</code>
   */
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG = "confluent.support.customer.id";
  private static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DOC = "Customer ID assigned by Confluent";
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT = "anonymous";

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

  private static final Pattern customerPattern = Pattern.compile("c\\d{1,30}");

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote anonymous users.
   */
  public static boolean isAnonymousUser(String customerId) {
    return customerId != null && customerId.toLowerCase().equals(CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the pattern of Confluent's internal customer ids.
   */
  public static boolean isConfluentCustomer(String customerId) {
    if (customerId != null) {
      return customerPattern.matcher(customerId.toLowerCase()).matches();
    } else {
      return false;
    }
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value is syntactically correct.
   */
  public static boolean isSyntacticallyCorrectCustomerId(String customerId) {
    return isAnonymousUser(customerId) || isConfluentCustomer(customerId);
  }

  public static String getCustomerId(Properties serverConfiguration) {
    String fallbackId = SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;
    String id = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    if (id == null || id.isEmpty()) {
      id = fallbackId;
    }
    if (!SupportConfig.isSyntacticallyCorrectCustomerId(id)) {
      log.error("'{}' is not a valid Confluent customer ID -- falling back to id '{}'", id, fallbackId);
      id = fallbackId;
    }
    return id;
  }

  public static long getReportIntervalMs(Properties serverConfiguration) {
    String intervalString = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG);
    if (intervalString == null || intervalString.isEmpty()) {
      intervalString = SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT;
    }
    try {
      long intervalHours = Long.parseLong(intervalString);
      if (intervalHours < 1) {
        throw new ConfigException(
            SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
            intervalString,
            "Interval must be >= 1");
      }
      return intervalHours * 60 * 60 * 1000;
    } catch (NumberFormatException e) {
      throw new ConfigException(
          SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
          intervalString,
          "Interval is not an integer number");
    }
  }

  public static String getKafkaTopic(Properties serverConfiguration) {
    String topic = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    if (topic == null) {
      return "";
    } else {
      return topic;
    }
  }

  public static String getKafkaBootstrapServers(KafkaServer server) {
    String hostname = server.config().advertisedHostName();
    Integer port = server.config().advertisedPort();
    return hostname + ":" + port.toString();
  }

  public static String getEndpointHTTP(Properties serverConfiguration) {
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "");
  }

  public static String getEndpointHTTPS(Properties serverConfiguration) {
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "");
  }
}
