package io.confluent.support.metrics;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import io.confluent.support.metrics.common.KafkaServerUtils;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import io.confluent.support.metrics.tools.KafkaMetricsToFile;
import kafka.utils.CoreUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration test. Verifies that metrics collected are successfully submitted to a Kafka topic
 */
public class MetricsToKafkaTest {
  private static KafkaServer server = null;
  private static KafkaServerUtils kafkaServerUtils = new KafkaServerUtils();
  @BeforeClass
  public static void startCluster() {
    server = kafkaServerUtils.startServer();
  }

  @AfterClass
  public static void stopCluster() {
    kafkaServerUtils.stopServer();
  }

  @Test
  public void testReceiveMetricsCollected() {
    Properties serverProperties = kafkaServerUtils.getDefaultBrokerConfiguration();
    // update it with actual zookeeper values
    serverProperties.remove(KafkaConfig$.MODULE$.ZkConnectProp());
    String zkConnect = kafkaServerUtils.getZookeeperConnection();
    serverProperties.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
    Runtime serverRuntime = Runtime.getRuntime();
    String topic = serverProperties.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);

    // disable sending to confluent to focus on just sending to a Kafka topic
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);
    String outputFile = "testFile.zip";

    int numMetricSubmissions = 10;


    // Collect metrics to the topic
    MetricsReporter reporter = new MetricsReporter(server, serverProperties, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    // verify that metrics are there
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(zkConnect);
    assertThat(kafkaMetricsToFile.collectMetrics(topic, outputFile)).isEqualTo(numMetricSubmissions);

    // cleanup
    kafkaMetricsToFile.getConsumer().shutdown();
    CoreUtils.rm(outputFile);
  }
}
