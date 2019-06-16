package org.apache.iotdb.tsfile.para.benchmark;

import java.util.NoSuchElementException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

public class UniformLoadGenerator implements LoadGenerator {

  private int deviceNum;
  private int sensorNum;
  private int timeLimit;
  private TSDataType dataType;

  private long idx = 0;

  public UniformLoadGenerator(BenchMarkConfig config) {
    this.deviceNum = config.getDeviceNum();
    this.sensorNum = config.getSensorNum();
    this.timeLimit = config.getPtNum();
    this.dataType = config.getDataType();
  }

  @Override
  public boolean hasNext() {
    return idx < deviceNum * timeLimit;
  }

  @Override
  public TSRecord next() {
    if (!hasNext()) {
      throw new NoSuchElementException(String.format("%d > %d", idx, deviceNum * timeLimit));
    }
    long time = idx / deviceNum;
    long deviceIdx = idx % deviceNum;
    String deviceId = "root.device" + deviceIdx;
    TSRecord record = new TSRecord(time, deviceId);
    for (int i = 0; i < sensorNum; i++) {
      String measurementId = "sensor" + i;
      String value = String.valueOf(time);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, value));
    }
    idx++;
    return record;
  }
}
