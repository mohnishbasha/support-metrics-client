package io.confluent.support.metrics;

import org.apache.kafka.common.utils.AppInfoParser;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import io.confluent.support.metrics.common.kafka.EmbeddedKafkaCluster;
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
  private static final Logger log = LoggerFactory.getLogger(MetricsToKafkaTest.class);
  private final Runtime serverRuntime = Runtime.getRuntime();
  private static Properties supportProperties = null;

  static {
    try {
      Properties props = new Properties();
      props.load(MetricsToKafkaTest.class.getResourceAsStream("/default-server.properties"));
      supportProperties = props;
    } catch (IOException e) {
      log.warn("Error while loading default properties:", e.getMessage());
    }
  }

  /**
   * Helper function that verifies most basic metrics
   *
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
   * Collects metrics to a Kafka topic and verifies that as many metrics are received as sent Only
   * one broker is used
   */
  @Test
  public void testReceiveMetricsNumCollected() {
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    Properties brokerSupportProps = (Properties) supportProperties.clone();
    brokerSupportProps.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(broker.config().brokerId()));
    brokerSupportProps.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), cluster.zookeeperConnectString());
    String topic = brokerSupportProps.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    // disable sending to confluent to focus on just sending to a Kafka topic
    brokerSupportProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    brokerSupportProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);
    String outputFile = "testFile.zip";
    int numMetricSubmissions = 10;
    int runTimeMs = 10 * 1000;

    // Collect metrics to the topic
    MetricsReporter reporter = new MetricsReporter(broker, brokerSupportProps, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    // verify that metrics are there
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(cluster.zookeeperConnectString(), runTimeMs);
    assertThat(kafkaMetricsToFile.collectMetrics(topic, outputFile)).isEqualTo(numMetricSubmissions);

    // cleanup
    kafkaMetricsToFile.getConsumer().shutdown();
    CoreUtils.rm(outputFile);

    // Cleanup
    cluster.stopCluster();
  }

  /**
   * Helper function that consumes messages from a topic with a timeout
   */
  private static void verifyConsume(String zkConnect, String topic, int numMetricSubmissions) throws IOException {
    AvroDeserializer decoder = new AvroDeserializer();
    int numBasicRecords = 0;
    int runTimeMs = 10 * 1000;
    // verify that metrics are there and deserialize them
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(zkConnect, runTimeMs);
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
   * Collects metrics to a Kafka topic and verifies the topic structure received matches what was
   * sent. Several brokers are available and the support topic is fully replicated
   */
  @Test
  public void testReceiveBasicMetricsEndtoEnd() throws IOException {
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    Properties brokerSupportProps = (Properties) supportProperties.clone();
    brokerSupportProps.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), new Integer(broker.config().brokerId()).toString());
    brokerSupportProps.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), cluster.zookeeperConnectString());
    String topic = brokerSupportProps.getProperty(SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    // disable sending to confluent to focus on just sending to a Kafka topic
    brokerSupportProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    brokerSupportProps.remove(SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);
    int numMetricSubmissions = 10;

    // Collect metrics to the topic from one broker server
    MetricsReporter reporter = new MetricsReporter(broker, brokerSupportProps, serverRuntime);
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    verifyConsume(cluster.zookeeperConnectString(), topic, numMetricSubmissions);

    // Cleanup
    cluster.stopCluster();
  }
}



