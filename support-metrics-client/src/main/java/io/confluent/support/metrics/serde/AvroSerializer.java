package io.confluent.support.metrics.serde;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializer {

  /**
   * Serializes the record as an in-memory representation of a standard Avro file.
   *
   * That is, the returned bytes include a standard Avro header that contains a magic byte, the
   * record's Avro schema (and so on), followed by the byte representation of the record.
   *
   * Implementation detail:  This method uses Avro's {@code DataFileWriter}.
   *
   * @return Avro-encoded record (bytes) that includes the Avro schema
   */
  public byte[] serialize(GenericContainer record) throws IOException {
    if (record != null) {
      DatumWriter<GenericContainer> datumWriter = new GenericDatumWriter<>(record.getSchema());
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataFileWriter<GenericContainer> writer = new DataFileWriter<>(datumWriter);
      writer.create(record.getSchema(), out);
      writer.append(record);
      writer.close();
      out.close();
      return out.toByteArray();
    } else {
      return null;
    }
  }

}