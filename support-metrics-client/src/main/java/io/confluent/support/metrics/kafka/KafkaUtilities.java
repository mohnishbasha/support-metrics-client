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
package io.confluent.support.metrics.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.cluster.Broker;
import kafka.common.TopicExistsException;
import kafka.log.LogConfig;
import kafka.server.BrokerShuttingDown;
import kafka.server.KafkaServer;
import kafka.server.PendingControlledShutdown;
import kafka.server.RunningAsBroker;
import kafka.server.RunningAsController;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaUtilities {

  private static final Logger log = LoggerFactory.getLogger(KafkaUtilities.class);

  public void createTopicIfMissing(ZkUtils zkUtils,
                                   String topic,
                                   int partitions,
                                   int replication,
                                   long retentionMs) {

    if (AdminUtils.topicExists(zkUtils, topic)) {
      verifySupportTopic(zkUtils, topic, partitions, replication);
      return;
    }

    Seq<Broker> brokerList = zkUtils.getAllBrokersInCluster();
    int actualReplication = Math.min(replication, brokerList.size());
    if (actualReplication < replication) {
      log.warn("The replication factor of topic {} will be set to {}, which is less than the " +
              "desired replication factor of {} (reason: this cluster contains only {} brokers).  " +
              "If you happen to add more brokers to this cluster, then it is important to increase " +
              "the replication factor of the topic to eventually {} to ensure reliable and  durable " +
              "metrics collection.",
          topic, actualReplication, replication, brokerList.size(),
          replication);
    }
    try {
      Properties metricsTopicProps = new Properties();
      metricsTopicProps.put(LogConfig.RetentionMsProp(), String.valueOf(retentionMs));
      log.info("Attempting to create topic {} with {} replicas, assuming {} total brokers",
          topic, actualReplication, brokerList.size());
      AdminUtils.createTopic(zkUtils, topic, partitions, actualReplication, metricsTopicProps);
    } catch (TopicExistsException te) {
      log.info("Topic {} already exists", topic);
    } catch (AdminOperationException e) {
      log.error("Could not create topic {}: {}", topic, e.getMessage());
    }
  }

  private void verifySupportTopic(ZkUtils zkUtils,
                                  String supportTopic,
                                  int partitions,
                                  int replication) {
    Set<String> topics = new HashSet<>();
    topics.add(supportTopic);
    scala.Option<scala.collection.Map<Object, Seq<Object>>> partitionAssignmentOption =
        zkUtils.getPartitionAssignmentForTopics(JavaConversions.asScalaSet(topics).
            toSeq()).get(supportTopic);
    if (!partitionAssignmentOption.isEmpty()) {
      scala.collection.Map partitionAssignment = partitionAssignmentOption.get();
      int actualNumPartitions = partitionAssignment.size();
      if (actualNumPartitions != partitions) {
        log.warn("The topic {} should have only {} partitions.  Having more " +
                "partitions should not hurt but it is only needed under special circumstances.",
            supportTopic, partitions);
      }
      int firstPartitionId = 0;
      scala.Option<Seq<Object>> replicasOfFirstPartitionOption =
          partitionAssignment.get(firstPartitionId);
      if (!replicasOfFirstPartitionOption.isEmpty()) {
        int actualReplication = replicasOfFirstPartitionOption.get().size();
        if (actualReplication < replication) {
          log.warn("The replication factor of topic {} is {}, which is less than " +
                  "the desired replication factor of {}.  If you happen to add more brokers to this " +
                  "cluster, then it is important to increase the replication factor of the topic to " +
                  "eventually {} to ensure reliable and durable metrics collection.",
              supportTopic, actualReplication, replication, replication);
        }
      } else {
        log.error("No replicas known for partition 0 of support metrics topic {}", supportTopic);
      }
    } else {
      log.error("No partitions are assigned to support metrics topic {}", supportTopic);
    }
  }

  public boolean isReadyForMetricsCollection(KafkaServer server) {
    return server.brokerState().currentState() == RunningAsBroker.state() ||
        server.brokerState().currentState() == RunningAsController.state();
  }

  public boolean isShuttingDown(KafkaServer server) {
    return server.brokerState().currentState() == PendingControlledShutdown.state() ||
        server.brokerState().currentState() == BrokerShuttingDown.state();
  }

}