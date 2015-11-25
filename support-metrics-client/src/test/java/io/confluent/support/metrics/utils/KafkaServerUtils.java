package io.confluent.support.metrics.utils;

import org.apache.kafka.common.protocol.SecurityProtocol;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

  public static String prepareDefaultConfig() throws IOException {
    String[] lines = {
        "broker.id=1", "zookeeper.connect=localhost:2181",
        SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG + "=anonymous",
        SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG + "=http://example.com",
        SupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG + "=https://example.com",
        SupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG + "=__sample_topic",
        SupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG + "=24"
    };
    return prepareConfig(lines);
  }

  public static String prepareConfig(String[] lines) throws IOException {
    File file = File.createTempFile("supportedkafkatest", ".properties");
    file.deleteOnExit();

    FileOutputStream out = new FileOutputStream(file);
    for (String line : lines) {
      out.write(line.getBytes());
      out.write("\n".getBytes());
    }
    out.close();
    return file.getAbsolutePath();
  }
}
