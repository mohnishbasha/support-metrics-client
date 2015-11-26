package io.confluent.support.metrics.submitters;

public interface Submitter {

  void submit(byte[] bytes);

}
