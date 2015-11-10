package io.confluent.support.metrics.collectors;

import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.utils.AppInfoParser;

import io.confluent.support.metrics.SupportKafkaMetricsBasic;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.TimeUtils;

public class BasicCollector implements Collector {

  private final TimeUtils time;

  public BasicCollector(TimeUtils time) {
    this.time = time;
  }

  /**
   * @return A new metrics record, or null in case of any errors.
   */
  @Override
  public GenericContainer collectMetrics() {
    SupportKafkaMetricsBasic metricsRecord = null;
    metricsRecord = new SupportKafkaMetricsBasic();
    metricsRecord.setTimestamp(time.nowInUnixTime());
    metricsRecord.setKafkaVersion(AppInfoParser.getVersion());
    // TODO: Correctly determine the CP version information.
    metricsRecord.setConfluentPlatformVersion(AppInfoParser.getVersion());
    return metricsRecord;
  }

}