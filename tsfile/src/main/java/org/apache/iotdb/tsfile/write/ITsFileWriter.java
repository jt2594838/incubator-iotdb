package org.apache.iotdb.tsfile.write;

import java.io.IOException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public interface ITsFileWriter {

  void close() throws IOException;

  void write(TSRecord record) throws IOException, WriteProcessException;

  void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException;
}
