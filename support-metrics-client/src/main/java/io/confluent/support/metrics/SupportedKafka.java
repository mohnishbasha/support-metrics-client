package io.confluent.support.metrics;

import java.util.Properties;

import kafka.Kafka;

/**
 * Starts a "supported" Kafka broker and any associated threads.
 *
 * This class is similar to Apache Kafka's {@code Kafka.scala}.  It differs mainly in that it starts
 * a {@link SupportedServerStartable} instead of a Apache Kafka's {@code KafkaServerStartable}.
 *
 * @see <a href="https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/Kafka.scala">Kafka.scala</a>
 */
public class SupportedKafka {

  public static void main(String[] args) throws Exception {
    try {
      Properties serverProps = Kafka.getPropsFromArgs(args);
      final SupportedServerStartable supportedServerStartable = new SupportedServerStartable(serverProps);

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          supportedServerStartable.shutdown();
        }
      });

      supportedServerStartable.startup();
      supportedServerStartable.awaitShutdown();
    } catch (Exception e) {
      System.exit(ExitCodes.ERROR);
    }
    System.exit(ExitCodes.SUCCESS);
  }

}
