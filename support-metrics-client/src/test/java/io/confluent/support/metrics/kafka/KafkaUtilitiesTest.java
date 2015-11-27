package io.confluent.support.metrics.kafka;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import io.confluent.support.metrics.common.KafkaServerUtils;
import io.confluent.support.metrics.utils.ExampleTopics;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import static org.assertj.core.api.Assertions.assertThat;
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

    assertThat(kUtil.verifySupportTopic(mockZkUtils, nullTopic, partitions, replication)).isFalse();
  }

  @Test
  public void testverifySupportTopicEmptyTopic()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";
    int partitions = 1;
    int replication = 1;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, emptyTopic, partitions, replication)).isFalse();
  }

  @Test
  public void testverifySupportTopicInvalidPartition()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 0;
    int replication = 1;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, anyTopic, partitions, replication)).isFalse();
  }

  @Test
  public void testverifySupportTopicInvalidReplication()
  {
    KafkaUtilities kUtil = new KafkaUtilities();
    String anyTopic = "valueNotRelevant";
    int partitions = 1;
    int replication = 0;

    assertThat(kUtil.verifySupportTopic(mockZkUtils, anyTopic, partitions, replication)).isFalse();
  }

  @Test
  public void testCreateExampleTopics()
  {
    KafkaUtilities kUtil = new KafkaUtilities();

    int partitions;
    int replication;
    long retentionMs = 365 * 24 * 60 * 60 * 1000L;

    for (String topic : ExampleTopics.exampleTopics) {
      partitions = rand.nextInt(10) + 1;
      replication = rand.nextInt(6) + 1;
      assertThat(kUtil.createTopicIfMissing(server.zkUtils(), topic, partitions, replication, retentionMs)).isTrue();
      assertThat(kUtil.verifySupportTopic(server.zkUtils(), topic, partitions, replication)).isTrue();
    }
  }
}
