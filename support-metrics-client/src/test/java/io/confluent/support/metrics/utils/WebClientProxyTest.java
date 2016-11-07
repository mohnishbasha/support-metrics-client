/**
 * Copyright 2016 Confluent Inc.
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
package io.confluent.support.metrics.utils;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockserver.integration.ClientAndProxy;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.socket.PortFactory;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.integration.ClientAndProxy.startClientAndProxy;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.verify.VerificationTimes.exactly;

/**
 * WebClient tests with proxy enabled
 * Note: this test is disabled for now since it will affect other tests if running the 'mvn test' command.
 * Running this test by itself would still work though.  Just comment out the ignore annotation below.
 * @see WebClientTest
 */
@Ignore
public class WebClientProxyTest {
  private static final String secureLiveTestEndpoint = "https://support-metrics.confluent.io/test";

  private static HttpHost proxy;
  private static ClientAndProxy clientAndProxy;
  private static CloseableHttpClient httpclient;
  private static SSLContext sslContext;
  private static HttpClientBuilder httpClientBuilder;

  private static ClientAndServer clientAndServer;
  private static int serverPort;

  @BeforeClass
  public static void startProxy() throws Exception {

    int port = PortFactory.findFreePort();
    clientAndProxy = startClientAndProxy(port);
    proxy = new HttpHost("localhost", port);
    sslContext = SSLContexts.custom().loadTrustMaterial(new File("src/test/resources/truststore.jks"), "changeit".toCharArray()).build();
    httpClientBuilder = HttpClients.custom().setSSLContext(sslContext);
    httpclient = httpClientBuilder.build();

    serverPort = PortFactory.findFreePort();
    clientAndServer = ClientAndServer.startClientAndServer(serverPort);
    clientAndServer.when(new HttpRequest().withMethod("GET")).respond(HttpResponse.response("OK"));
  }

  @AfterClass
  public static void stopProxy() throws IOException {
    if (clientAndServer.isRunning()) clientAndServer.stop();
    if (clientAndProxy.isRunning()) clientAndProxy.stop();
    try {
      httpclient.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void resetProxy() {
    if ((clientAndProxy != null) && clientAndProxy.isRunning()) clientAndProxy.reset();
  }

  @Test
  public void testNetworkProxyWithHttpClient() throws IOException {
    // Given
    HttpHost target = new HttpHost("localhost", serverPort);
    HttpGet g = new HttpGet("/");
    RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
    g.setConfig(config);
    CloseableHttpClient httpclient = HttpClients.createDefault();
    // http test
    try (CloseableHttpResponse response = httpclient.execute(target, g)) {
      Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
      clientAndProxy.verify(request().withPath("/").withMethod("GET"), exactly(1));
      clientAndServer.verify(request().withPath("/").withMethod("GET"), exactly(1));
    }

    // https test
    target = new HttpHost("localhost", serverPort, "https");
    try {
      httpclient = HttpClients.custom().setSSLContext(sslContext).build();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try (CloseableHttpResponse response = httpclient.execute(target, g)) {
      Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
      clientAndProxy.verify(request().withPath("/").withMethod("GET"), exactly(2));
      clientAndServer.verify(request().withPath("/").withMethod("GET"), exactly(2));
      clientAndServer.dumpToLog();
    }
    finally {
      httpclient.close();
    }
  }

  @Test
  public void testNetworkProxy() throws NoSuchAlgorithmException {
    // Given
    HttpPost p = new HttpPost(secureLiveTestEndpoint);
    byte[] anyData = "anyData".getBytes();

    // When/Then
    int status = WebClient.send(CustomerIdExamples.validCustomerIds[0], anyData, p, proxy, httpclient);

    assertThat(status == HttpStatus.SC_OK).isTrue();
    clientAndProxy.verify(request().withMethod("POST").withPath("/test"), exactly(1));
  }

}
