package io.confluent.support.metrics;



import org.apache.kafka.common.utils.AppInfoParser;

import org.junit.Test;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.KafkaServerUtils;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;
import io.confluent.support.metrics.serde.AvroDeserializer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import io.confluent.support.metrics.tools.KafkaMetricsToFile;
import kafka.utils.CoreUtils;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Integration test. Verifies that metrics collected are successfully submitted to a Kafka topic
 */
public class MetricsToKafkaTest {
  private static KafkaServer server = null;
  private static KafkaServerUtils kafkaServerUtils = new KafkaServerUtils();
  private final Runtime serverRuntime = Runtime.getRuntime();

  /**
   * Helper function that verifies most basic metrics
   * @param basicRecord Record to be verified
   */
  public static void verifyBasicMetrics(SupportKafkaMetricsBasic basicRecord) {
    TimeUtils time = new TimeUtils();
    assertThat(basicRecord.getTimestamp()).isLessThanOrEqualTo(time.nowInUnixTime());
    assertThat(basicRecord.getKafkaVersion()).isEqualTo(AppInfoParser.getVersion());
    assertThat(basicRecord.getConfluentPlatformVersion()).isEqualTo(Version.getVersion());
    assertThat(basicRecord.getBrokerProcessUUID()).isNotEmpty();
  }


  /**
   * Collects metrics to a Kafka topic and verifies that as many metrics are received as sent
   * Only one broker is available, thus the support topic is not replicated
   */
  @Test
  public void testReceiveMetricsNumCollected() {
    server = kafkaServerUtils.startServer();
    Properties serverProperties = kafkaServerUtils.getDefaultBrokerConfiguration();
    // update it with actual zookeeper values
    serverProperties.remove(KafkaConfig$.MODULE$.ZkConnectProp());
    String zkConnect = kafkaServerUtils.getZookeeperConnection();
    serverProperties.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
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
    // stop the cluster so that the topics are deleted and other tests are not impacted
    kafkaServerUtils.stopServer();
  }

  /**
   * Helper function that consumes messages from a topic with a timeout
   * @param zkConnect
   * @param topic
   * @param numMetricSubmissions
   * @throws IOException
   */
  private static void verifyConsume(String zkConnect, String topic, int numMetricSubmissions) throws IOException {
    AvroDeserializer decoder = new AvroDeserializer();
    int numBasicRecords = 0;
    // verify that metrics are there and deserialize them
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(zkConnect);
    final List<KafkaStream<byte[], byte[]>> streams = kafkaMetricsToFile.getStreams(topic);

    try {
      for (final KafkaStream stream : streams) {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {

          SupportKafkaMetricsBasic[] container = decoder.deserialize(SupportKafkaMetricsBasic.class, it.next().message());
          assertThat(container.length).isEqualTo(1);
          verifyBasicMetrics(container[0]);
          numBasicRecords++;
        }
      }
    } catch (ConsumerTimeoutException e) {
      // do nothing, this is expected success case since we consume with a timeout
    }

    assertThat(numBasicRecords).isEqualTo(numMetricSubmissions);
    // cleanup
    kafkaMetricsToFile.getConsumer().shutdown();
  }

  /**
   * Collects metrics to a Kafka topic and verifies the topic structure received matches what was sent.
   * Several brokers are available and the support topic is fully replicated
   * @throws IOException
   */
  @Test
  public void testReceiveBasicMetricsEndtoEnd() throws IOException {
    // start three brokers so that support topic is fully replicated
    KafkaServer[] servers = new KafkaServer[MetricsReporter.SUPPORT_TOPIC_REPLICATION];
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      servers[i] = kafkaServerUtils.startServer(i);
    }
    AvroDeserializer decoder = new AvroDeserializer();
    Properties serverProperties = kafkaServerUtils.getDefaultBrokerConfiguration();
    // update it with actual zookeeper values
    serverProperties.remove(KafkaConfig$.MODULE$.ZkConnectProp());
    String zkConnect = kafkaServerUtils.getZookeeperConnection();
    serverProperties.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
    // update record to collect basic metrics
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "anonymous");
    String topic = serverProperties.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    int numMetricSubmissions = 10;
    // disable sending to confluent to focus on just sending to a Kafka topic
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);


    // Collect metrics to the topic from one broker server
    MetricsReporter reporter = new MetricsReporter(servers[0], serverProperties, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    verifyConsume(zkConnect, topic, numMetricSubmissions);

    // stop the cluster so that the topics are deleted and other tests are not impacted
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      kafkaServerUtils.stopServer(i);
    }
  }



  /**
   * Collects metrics to a Kafka topic and verifies the topic structure received matches what was sent.
   * Several brokers are available and the support topic is fully replicated, but N-1 brokers fail after
   * data is produced
   * @throws IOException
   */
  @Test
  public void testReceiveBasicMetricsEndtoEndBrokerFailureAfterProduction() throws IOException {
    // start three brokers so that support topic is fully replicated
    KafkaServer[] servers = new KafkaServer[MetricsReporter.SUPPORT_TOPIC_REPLICATION];
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      servers[i] = kafkaServerUtils.startServer(i);
    }
    Properties serverProperties = kafkaServerUtils.getDefaultBrokerConfiguration();
    // update it with actual zookeeper values
    serverProperties.remove(KafkaConfig$.MODULE$.ZkConnectProp());
    String zkConnect = kafkaServerUtils.getZookeeperConnection();
    serverProperties.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
    // update record to collect basic metrics
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "anonymous");
    String topic = serverProperties.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    int numMetricSubmissions = 10;
    // disable sending to confluent to focus on just sending to a Kafka topic
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);


    // Collect metrics to the topic from one broker server
    MetricsReporter reporter = new MetricsReporter(servers[0], serverProperties, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    //
    // brokers fail
    //
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION - 1; i++) {
      kafkaServerUtils.stopServer(i);
    }

    verifyConsume(zkConnect, topic, numMetricSubmissions);

    // stop the cluster so that the topics are deleted and other tests are not impacted
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      kafkaServerUtils.stopServer(i);
    }
  }

  /**
   * Collects metrics to a Kafka topic and verifies the topic structure received matches what was sent.
   * Several brokers are available and the support topic is fully replicated, but N-1 brokers fail before
   * data is produced
   * @throws IOException
   */
  @Test
  public void testReceiveBasicMetricsEndtoEndBrokerFailureBeforeProduction() throws IOException {
    // start three brokers so that support topic is fully replicated
    KafkaServer[] servers = new KafkaServer[MetricsReporter.SUPPORT_TOPIC_REPLICATION];
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      servers[i] = kafkaServerUtils.startServer(i);
    }
    Properties serverProperties = kafkaServerUtils.getDefaultBrokerConfiguration();
    // update it with actual zookeeper values
    serverProperties.remove(KafkaConfig$.MODULE$.ZkConnectProp());
    String zkConnect = kafkaServerUtils.getZookeeperConnection();
    serverProperties.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
    // update record to collect basic metrics
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    serverProperties.setProperty(SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "anonymous");
    String topic = serverProperties.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    int numMetricSubmissions = 10;
    // disable sending to confluent to focus on just sending to a Kafka topic
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    serverProperties.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);

    //
    // brokers fail
    //
    for (int i = 1; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      kafkaServerUtils.stopServer(i);
    }

    // Collect metrics to the topic from one broker server
    MetricsReporter reporter = new MetricsReporter(servers[0], serverProperties, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    verifyConsume(zkConnect, topic, numMetricSubmissions);

    // stop the cluster so that the topics are deleted and other tests are not impacted
    for (int i = 0; i < MetricsReporter.SUPPORT_TOPIC_REPLICATION; i++) {
      kafkaServerUtils.stopServer(i);
    }
  }
}
