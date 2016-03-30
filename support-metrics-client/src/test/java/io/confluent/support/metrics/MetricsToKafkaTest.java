package io.confluent.support.metrics;

import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.kafka.EmbeddedKafkaCluster;
import io.confluent.support.metrics.common.time.TimeUtils;
import io.confluent.support.metrics.serde.AvroDeserializer;
import io.confluent.support.metrics.tools.KafkaMetricsToFile;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Integration test.  Verifies that collected metrics are successfully submitted to a Kafka topic.
 */
public class MetricsToKafkaTest {

  @Test
  public void savesAsManyMetricsToFileAsHaveBeenSubmittedBySingleNodeCluster() throws IOException {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    Properties brokerConfiguration = SupportConfig.mergeAndValidateWithDefaultProperties(defaultBrokerConfiguration(broker, cluster.zookeeperConnectString()));
    brokerConfiguration.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    String topic = brokerConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    int timeoutMs = 10 * 1000;
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(cluster.zookeeperConnectString(), timeoutMs);

    // Sent metrics to the topic
    int numMetricSubmissions = 10;
    MetricsReporter reporter = new MetricsReporter(broker, brokerConfiguration, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    // When/Then
    String outputFile = "testFile.zip";
    assertThat(kafkaMetricsToFile.saveMetricsToFile(topic, outputFile)).isEqualTo(numMetricSubmissions);

    // Cleanup
    kafkaMetricsToFile.shutdown();
    List<String> outputFiles = Arrays.asList(outputFile);
    CoreUtils.delete(scala.collection.JavaConversions.asScalaBuffer(outputFiles).seq());
    cluster.stopCluster();
  }

  private Properties defaultBrokerConfiguration(KafkaServer broker, String zookeeperConnect) throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(MetricsToKafkaTest.class.getResourceAsStream("/default-server.properties"));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(broker.config().brokerId()));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeperConnect);

    return brokerConfiguration;
  }

  @Test
  public void retrievesBasicMetricsSubmittedByMultiNodeCluster() throws IOException {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer firstBroker = cluster.getBroker(0);

    Properties brokerConfiguration = SupportConfig.mergeAndValidateWithDefaultProperties(defaultBrokerConfiguration(firstBroker, cluster.zookeeperConnectString()));
    brokerConfiguration.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");

    MetricsReporter reporter = new MetricsReporter(firstBroker, brokerConfiguration, serverRuntime);
    String topic = brokerConfiguration.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);

    // When
    int expNumMetricSubmissions = 10;
    for (int i = 0; i < expNumMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    // Then
    verifyMetricsSubmittedToTopic(cluster.zookeeperConnectString(), topic, expNumMetricSubmissions);

    // Cleanup
    cluster.stopCluster();
  }

  /**
   * Helper function that consumes messages from a topic with a timeout.
   */
  private static void verifyMetricsSubmittedToTopic(
      String zkConnect,
      String topic,
      int expNumMetricSubmissions) throws IOException {
    int timeoutMs = 10 * 1000;
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(zkConnect, timeoutMs);
    List<KafkaStream<byte[], byte[]>> streams = kafkaMetricsToFile.getStreams(topic);

    int numRecords = 0;
    AvroDeserializer decoder = new AvroDeserializer();
    try {
      for (final KafkaStream<byte[], byte[]> stream : streams) {
        for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
          SupportKafkaMetricsBasic[] container = decoder.deserialize(SupportKafkaMetricsBasic.class,
              messageAndMetadata.message());
          assertThat(container.length).isEqualTo(1);
          verifyBasicMetrics(container[0]);
          numRecords++;
        }
      }
    } catch (ConsumerTimeoutException e) {
      // do nothing, this is expected success case since we consume with a timeout
    }

    assertThat(numRecords).isEqualTo(expNumMetricSubmissions);

    // Cleanup
    kafkaMetricsToFile.shutdown();
  }

  private static void verifyBasicMetrics(SupportKafkaMetricsBasic basicRecord) {
    TimeUtils time = new TimeUtils();
    assertThat(basicRecord.getTimestamp()).isLessThanOrEqualTo(time.nowInUnixTime());
    assertThat(basicRecord.getKafkaVersion()).isEqualTo(AppInfoParser.getVersion());
    assertThat(basicRecord.getConfluentPlatformVersion()).isEqualTo(Version.getVersion());
    assertThat(basicRecord.getBrokerProcessUUID()).isNotEmpty();
  }

}



