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

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.http.HttpStatus;
import io.confluent.support.metrics.utils.WebClient;
import io.confluent.support.metrics.SupportConfig;

import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentSubmitter implements Submitter {

  private static final Logger log = LoggerFactory.getLogger(ConfluentSubmitter.class);

  private final String customerId;
  private final String endpointHTTP;
  private final String endpointHTTPS;

  /**
   * Class that decides how to send data to Confluent.
   *
   * @param endpointHTTP:  HTTP endpoint for the Confluent support service. Can be null.
   * @param endpointHTTPS: HTTPS endpoint for the Confluent support service. Can be null.
   */
  public ConfluentSubmitter(String customerId, String endpointHTTP, String endpointHTTPS) {

    if ((endpointHTTP == null || endpointHTTP.isEmpty()) && (endpointHTTPS == null || endpointHTTPS.isEmpty())) {
      throw new IllegalArgumentException("must specify endpoints");
    }
    if (endpointHTTP != null && !endpointHTTP.isEmpty()) {
      if (!testEndpointValid(new String[]{"http"}, endpointHTTP)) {
        throw new IllegalArgumentException("invalid HTTP endpoint");
      }
    }
    if (endpointHTTPS != null && !endpointHTTPS.isEmpty()) {
      if (!testEndpointValid(new String[]{"https"}, endpointHTTPS)) {
        throw new IllegalArgumentException("invalid HTTPS endpoint");
      }
    }
    if (!SupportConfig.isSyntacticallyCorrectCustomerId(customerId)) {
      throw new IllegalArgumentException("invalid customer ID");
    }
    this.endpointHTTP = endpointHTTP;
    this.endpointHTTPS = endpointHTTPS;
    this.customerId = customerId;
  }

  private boolean testEndpointValid(String[] schemes, String endpoint) {
    UrlValidator urlValidator = new UrlValidator(schemes);
    if (urlValidator.isValid(endpoint)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Submits metrics to Confluent via the Internet.  Ignores null or empty inputs.
   */
  @Override
  public void submit(byte[] bytes) {
    if (bytes != null && bytes.length > 0) {
      int statusCode = WebClient.DEFAULT_STATUS_CODE;
      if (isSecureEndpointEnabled()) {
        statusCode = sendSecurely(bytes);
        if (!submittedSuccessfully(statusCode)) {
          if (isInsecureEndpointEnabled()) {
            log.error("Failed to submit metrics via secure endpoint, falling back to insecure endpoint");
            submitToInsecureEndpoint(bytes);
          } else {
            log.error("Failed to submit metrics via secure endpoint -- giving up");
          }
        } else {
          log.info("Successfully submitted metrics to Confluent via secure endpoint");
        }
      } else {
        if (isInsecureEndpointEnabled()) {
          submitToInsecureEndpoint(bytes);
        } else {
          log.error("Metrics will not be submitted because all endpoints are disabled");
        }
      }
    } else {
      log.error("Could not submit metrics to Confluent (metrics data missing)");
    }
  }

  private void submitToInsecureEndpoint(byte[] encodedMetricsRecord) {
    int statusCode = sendInsecurely(encodedMetricsRecord);
    if (submittedSuccessfully(statusCode)) {
      log.info("Successfully submitted metrics to Confluent via insecure endpoint");
    } else {
      log.error("Failed to submit metrics to Confluent via insecure endpoint -- giving up");
    }
  }

  private boolean isSecureEndpointEnabled() {
    return !endpointHTTPS.isEmpty();
  }

  private boolean isInsecureEndpointEnabled() {
    return !endpointHTTP.isEmpty();
  }

  private boolean submittedSuccessfully(int statusCode) {
    return statusCode == HttpStatus.SC_OK;
  }

  private int sendSecurely(byte[] encodedMetricsRecord) {
    return send(encodedMetricsRecord, endpointHTTPS);
  }

  private int sendInsecurely(byte[] encodedMetricsRecord) {
    return send(encodedMetricsRecord, endpointHTTP);
  }

  private int send(byte[] encodedMetricsRecord, String endpoint) {
    return WebClient.send(customerId, encodedMetricsRecord, new HttpPost(endpoint));
  }
}