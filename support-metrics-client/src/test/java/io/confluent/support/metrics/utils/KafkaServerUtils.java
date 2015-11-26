package io.confluent.support.metrics.utils;

import org.apache.kafka.common.protocol.SecurityProtocol;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;

import io.confluent.support.metrics.SupportConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option;
import scala.Option$;

public class KafkaServerUtils {

  public static EmbeddedZookeeper startZookeeper() {
    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    return zookeeper;
  }

  public static void stopZookeeper(EmbeddedZookeeper zookeeper) {
    if (zookeeper == null) {
      return;
    }
    zookeeper.shutdown();
  }

  public static KafkaServer startServer(EmbeddedZookeeper zookeeper) {
    if (zookeeper == null) {
      return null;
    }

    int brokerId = 0;
    Option<SecurityProtocol> so = Option.apply(SecurityProtocol.PLAINTEXT);
    Properties props = TestUtils.createBrokerConfig(brokerId, "localhost:" + zookeeper.port(), true, false, 2181,
        so, Option$.MODULE$.<File>empty(), true, false, 0, false, 0, false, 0);
    KafkaServer server = TestUtils.createServer(KafkaConfig.fromProps(props), SystemTime$.MODULE$);

    return server;
  }

  public static void stopServer(KafkaServer server) {
    if (server == null) {
      return;
    }
    server.shutdown();
    CoreUtils.rm(server.config().logDirs());
  }

  public static String pathToDefaultBrokerConfiguration() {
    return KafkaServerUtils.class.getResource("/test-server.properties").getFile();
  }

}
