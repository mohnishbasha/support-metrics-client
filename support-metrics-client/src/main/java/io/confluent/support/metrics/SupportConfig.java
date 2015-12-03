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



/**
 * Configuration for the Confluent Support options.
 *
 * Note: These Confluent-specific settings are added to {@code config/server.properties} by
 * Confluent's <a href="https://github.com/confluentinc/kafka-packaging">kafka-packaging</a> via a
 * patch file.  If you need to make any changes (e.g. renaming settings, adding/removing settings),
 * then make sure to also update the patch file accordingly.
 */
public class SupportConfig {

  private static final Logger log = LoggerFactory.getLogger(SupportConfig.class);

  /**
   * <code>confluent.support.metrics.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG = "confluent.support.metrics.enable";
  private static final String CONFLUENT_SUPPORT_METRICS_ENABLE_DOC = "False to disable metric collection, true otherwise.";
  public static final String CONFLUENT_SUPPORT_METRICS_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.customer.id</code>
   */
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG = "confluent.support.customer.id";
  private static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DOC = "Customer ID assigned by Confluent";
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT = "anonymous";
  public static final String CONFLUENT_SUPPORT_TEST_ID_DEFAULT = "c0";

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
  public static final String CONFLUENT_SUPPORT_METRICS_TOPIC_DEFAULT = "__confluent.support.metrics";
  /**
   * <code>confluent.support.metrics.endpoint.insecure</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG = "confluent.support.metrics.endpoint.insecure";
  private static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DOC = "Confluent endpoint that receives metrics over HTTP";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DEFAULT = "http://support-metrics.confluent.io/anon";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CUSTOMER_DEFAULT = "http://support-metrics.confluent.io/submit";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_TEST_DEFAULT = "http://support-metrics.confluent.io/test";

  /**
   * <code>confluent.support.metrics.endpoint.secure</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG = "confluent.support.metrics.endpoint.secure";
  private static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DOC = "Confluent endpoint that receives metrics over HTTPS";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DEFAULT = "https://support-metrics.confluent.io/anon";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CUSTOMER_DEFAULT = "https://support-metrics.confluent.io/submit";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_TEST_DEFAULT = "https://support-metrics.confluent.io/test";

  private static final Pattern customerPattern = Pattern.compile("c\\d{1,30}");

  /**
   * Returns the default Proactive Support properties
   * @return
   */
  public static Properties getDefaultProps() {
    Properties props = new Properties();
    props.setProperty(CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, CONFLUENT_SUPPORT_METRICS_ENABLE_DEFAULT);
    props.setProperty(CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
    props.setProperty(CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG, CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT);
    props.setProperty(CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, CONFLUENT_SUPPORT_METRICS_TOPIC_DEFAULT);
    props.setProperty(CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DEFAULT);
    props.setProperty(CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DEFAULT);

    return props;
  }

  /**
   * Takes default properties from getDefaultProps() and a set of override properties and
   * returns a merged properties object, where the defaults are overriden.
   * Performs some basic validation on the properties returned
   * @param defaults
   * @param overrides
   * @return
   */
  public static Properties mergeAndValidateProperties(Properties defaults, Properties overrides) {
    if (overrides == null) {
      return defaults;
    }
    if (defaults == null) {
      return overrides;
    }
    Properties props = new Properties();
    props.putAll(defaults);
    props.putAll(overrides);

    // set the correct customer id/endpoint pair
    if (isAnonymousUser(getCustomerId(props))) {
      if (!getEndpointHTTP(props).isEmpty()) {
        setEndpointHTTP(CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DEFAULT, props);
      }
      if (!getEndpointHTTPS(props).isEmpty()) {
        setEndpointHTTPS(CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DEFAULT, props);
      }
    } else if (isTestUser(getCustomerId(props))) {
      if (!getEndpointHTTP(props).isEmpty()) {
        setEndpointHTTP(CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_TEST_DEFAULT, props);
      }
      if (!getEndpointHTTPS(props).isEmpty()) {
        setEndpointHTTPS(CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_TEST_DEFAULT, props);
      }
    }
    else {
      if (!getEndpointHTTP(props).isEmpty()) {
        setEndpointHTTP(CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CUSTOMER_DEFAULT, props);
      }
      if (!getEndpointHTTPS(props).isEmpty()) {
        setEndpointHTTPS(CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CUSTOMER_DEFAULT, props);
      }
    }

    return props;
  }

  /**
   * A check on whether Proactive Support (PS) is enabled or not. PS is disabled when
   * CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG is false or is not defined
   *
   * @return false if PS is not enabled, true if PS is enabled
   */
  public static boolean isProactiveSupportEnabled(Properties serverConfiguration) {
    if (serverConfiguration == null) {
      return false;
    }
   return getMetricsEnabled(serverConfiguration);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote anonymous users.
   */
  public static boolean isAnonymousUser(String customerId) {
    return customerId != null && customerId.toLowerCase().equals(CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote internal testing.
   */
  public static boolean isTestUser(String customerId) {
    return customerId != null && customerId.toLowerCase().equals(CONFLUENT_SUPPORT_TEST_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the pattern of Confluent's internal customer ids.
   */
  public static boolean isConfluentCustomer(String customerId) {
    return customerId != null && customerPattern.matcher(customerId.toLowerCase()).matches();
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
      log.error("No customer ID configured -- falling back to id '{}'", fallbackId);
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
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "");
  }

  public static boolean getMetricsEnabled(Properties serverConfiguration) {
    String enableString = serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    return Boolean.parseBoolean(enableString);
  }

  public static String getEndpointHTTP(Properties serverConfiguration) {
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "");
  }

  public static void setEndpointHTTP(String endpointHTTP, Properties serverConfiguration) {
     serverConfiguration.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, endpointHTTP);
  }

  public static String getEndpointHTTPS(Properties serverConfiguration) {
    return serverConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "");
  }

  public static void setEndpointHTTPS(String endpointHTTPS, Properties serverConfiguration) {
    serverConfiguration.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, endpointHTTPS);
  }
}
