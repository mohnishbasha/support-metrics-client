package io.confluent.support.metrics.kafka;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import io.confluent.support.metrics.common.KafkaServerUtils;
import io.confluent.support.metrics.kafka.KafkaUtilities.VerifyTopicState;
import io.confluent.support.metrics.utils.ExampleTopics;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class KafkaUtilitiesTest {
  private static KafkaServer server = null;
  private ZkUtils mockZkUtils = mock(ZkUtils.class);
  private Random rand = new Random();
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
  public void testCreateTopicIfMissingNullZkUtils()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 1;
    int replication = 1;
    long retentionMs = 1000;

    assertThat(kUtil.createTopicIfMissing(null, anyTopic, partitions, replication, retentionMs)).isFalse();
  }

  @Test
  public void testCreateTopicIfMissingNullTopic()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;
    int partitions = 1;
    int replication = 1;
    long retentionMs = 1000;

    assertThat(kUtil.createTopicIfMissing(mockZkUtils, nullTopic, partitions, replication, retentionMs)).isFalse();
  }

  @Test
  public void testCreateTopicIfMissingEmptyTopic()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";
    int partitions = 1;
    int replication = 1;
    long retentionMs = 1000;

    assertThat(kUtil.createTopicIfMissing(mockZkUtils, emptyTopic, partitions, replication, retentionMs)).isFalse();
  }

  @Test
  public void testCreateTopicIfMissingInvalidPartition()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 0;
    int replication = 1;
    long retentionMs = 1000;

    assertThat(kUtil.createTopicIfMissing(mockZkUtils, anyTopic, partitions, replication, retentionMs)).isFalse();
  }

  @Test
  public void testCreateTopicIfMissingInvalidReplication()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 1;
    int replication = 0;
    long retentionMs = 1000;

    assertThat(kUtil.createTopicIfMissing(mockZkUtils, anyTopic, partitions, replication, retentionMs)).isFalse();
  }

  @Test
  public void testCreateTopicIfMissingInvalidRetention()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 1;
    int replication = 1;
    long retentionMs = 0;

    assertThat(kUtil.createTopicIfMissing(mockZkUtils, anyTopic, partitions, replication, retentionMs)).isFalse();
  }


  @Test
  public void testverifySupportTopicNullTopic()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;
    int partitions = 1;
    int replication = 1;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, nullTopic, partitions, replication)).isEqualTo(VerifyTopicState.Inadequate);
  }

  @Test
  public void testverifySupportTopicEmptyTopic()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";
    int partitions = 1;
    int replication = 1;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, emptyTopic, partitions, replication)).isEqualTo(VerifyTopicState.Inadequate);
  }

  @Test
  public void testverifySupportTopicInvalidPartition()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 0;
    int replication = 1;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, anyTopic, partitions, replication)).isEqualTo(VerifyTopicState.Inadequate);
  }

  @Test
  public void testverifySupportTopicInvalidReplication()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 1;
    int replication = 0;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, anyTopic, partitions, replication)).isEqualTo(VerifyTopicState.Inadequate);
  }

  @Test
  public void testCreateExampleTopics()
  {
    KafkaUtilities kUtil = new KafkaUtilities();

    int partitions;
    int replication;
    long retentionMs = 365 * 24 * 60 * 60 * 1000L;

    for (String topic : ExampleTopics.exampleTopics) {
      partitions = rand.nextInt(10) + 2;
      replication = rand.nextInt(6) + 2;
      assertThat(kUtil.createTopicIfMissing(server.zkUtils(), topic, partitions, replication, retentionMs)).isTrue();
      assertThat(kUtil.verifySupportTopic(server.zkUtils(), topic, partitions, replication)).isEqualTo(VerifyTopicState.Less);
    }
  }

  @Test
  public void testCreateExampleTopicsRecreate()
  {
    KafkaUtilities kUtil = new KafkaUtilities();

    int partitions;
    int replication;
    long retentionMs = 365 * 24 * 60 * 60 * 1000L;

    for (String topic : ExampleTopics.exampleTopics) {
      partitions = rand.nextInt(10) + 2;
      replication = rand.nextInt(6) + 2;
      assertThat(kUtil.createTopicIfMissing(server.zkUtils(), topic, partitions, replication, retentionMs)).isTrue();
      assertThat(kUtil.createTopicIfMissing(server.zkUtils(), topic, partitions, replication, retentionMs)).isTrue();
      assertThat(kUtil.verifySupportTopic(server.zkUtils(), topic, partitions, replication)).isEqualTo(VerifyTopicState.Less);
    }
  }

  @Test
  public void testCreateExampleTopicsNoLiveBrokers() {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 1;
    int replication = 1;
    long retentionMs = 1000L;
    boolean success = false;

    // stop the only broker we have
    kafkaServerUtils.stopServer();

    success = kUtil.createTopicIfMissing(server.zkUtils(), anyTopic, partitions, replication, retentionMs);

    // restart broker for other tests
    kafkaServerUtils.startServer();

    assertThat(success).isEqualTo(false);
  }
}
