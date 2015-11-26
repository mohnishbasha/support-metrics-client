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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import io.confluent.support.metrics.SupportConfig;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConfluentSubmitter implements Submitter {

  private static final Logger log = LoggerFactory.getLogger(ConfluentSubmitter.class);

  private static final int requestTimeoutMs = 2000;
  private static final int DEFAULT_STATUS_CODE = HttpStatus.SC_BAD_GATEWAY;
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
      int statusCode = DEFAULT_STATUS_CODE;
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
    return send(encodedMetricsRecord, new HttpPost(endpoint));
  }

  // This method is `protected` instead of `private` to be visible for testing.
  protected int send(byte[] bytes, HttpPost httpPost) {
    int statusCode = DEFAULT_STATUS_CODE;
    if (bytes != null && bytes.length > 0 && httpPost != null) {

      // add the body to the request
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
      builder.addTextBody("cid", customerId);
      builder.addBinaryBody("file", bytes, ContentType.DEFAULT_BINARY, "filename");
      httpPost.setEntity(builder.build());

      // set the HTTP config
      final RequestConfig config = RequestConfig.custom().
          setConnectTimeout(requestTimeoutMs).
          setConnectionRequestTimeout(requestTimeoutMs).
          setSocketTimeout(requestTimeoutMs).
          build();

      // send request
      try (CloseableHttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
           CloseableHttpResponse response = httpclient.execute(httpPost)) {
        log.debug("POST request returned {}", response.getStatusLine().toString());
        statusCode = response.getStatusLine().getStatusCode();
      } catch (IOException e) {
        log.debug("Could not submit metrics to Confluent: {}", e.getMessage());
      }
    }
    return statusCode;
  }
}