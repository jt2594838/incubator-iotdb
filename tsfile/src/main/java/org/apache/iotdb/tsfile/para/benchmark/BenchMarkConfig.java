package org.apache.iotdb.tsfile.para.benchmark;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.para.HashFunc;

public class BenchMarkConfig {
  private boolean useParallel = true;

  private String[] filePaths = new String[] {
    "tsfile1","tsfile2","tsfile3","tsfile4","tsfile5"
  };

  private int deviceNum = 100;
  private int sensorNum = 100;
  private int ptNum = 100;
  private TSDataType dataType = TSDataType.DOUBLE;
  private TSEncoding encoding = TSEncoding.PLAIN;

  private double deviceSelectRatio = 0.1;
  private double sensorSelectRatio = 0.1;
  private double timeSelectRatio = 0.1;
  private int queryNum = 100;

  private HashFunc hashFunc = String::hashCode;

  public boolean isUseParallel() {
    return useParallel;
  }

  public void setUseParallel(boolean useParallel) {
    this.useParallel = useParallel;
  }

  public String[] getFilePaths() {
    return filePaths;
  }

  public void setFilePaths(String[] filePaths) {
    this.filePaths = filePaths;
  }

  public int getDeviceNum() {
    return deviceNum;
  }

  public void setDeviceNum(int deviceNum) {
    this.deviceNum = deviceNum;
  }

  public int getSensorNum() {
    return sensorNum;
  }

  public void setSensorNum(int sensorNum) {
    this.sensorNum = sensorNum;
  }

  public int getPtNum() {
    return ptNum;
  }

  public void setPtNum(int ptNum) {
    this.ptNum = ptNum;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public HashFunc getHashFunc() {
    return hashFunc;
  }

  public void setHashFunc(HashFunc hashFunc) {
    this.hashFunc = hashFunc;
  }

  public double getDeviceSelectRatio() {
    return deviceSelectRatio;
  }

  public void setDeviceSelectRatio(double deviceSelectRatio) {
    this.deviceSelectRatio = deviceSelectRatio;
  }

  public double getSensorSelectRatio() {
    return sensorSelectRatio;
  }

  public void setSensorSelectRatio(double sensorSelectRatio) {
    this.sensorSelectRatio = sensorSelectRatio;
  }

  public double getTimeSelectRatio() {
    return timeSelectRatio;
  }

  public void setTimeSelectRatio(double timeSelectRatio) {
    this.timeSelectRatio = timeSelectRatio;
  }

  public int getQueryNum() {
    return queryNum;
  }

  public void setQueryNum(int queryNum) {
    this.queryNum = queryNum;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  @SuppressWarnings("Duplicates")
  public String toString() {
    List<String> fields = new ArrayList<>();
    fields.add(String.valueOf(useParallel));
    fields.add(String.valueOf(filePaths.length));
    fields.add(String.valueOf(deviceNum));
    fields.add(String.valueOf(sensorNum));
    fields.add(String.valueOf(ptNum));
    fields.add(String.valueOf(dataType));
    fields.add(String.valueOf(encoding));
    fields.add(String.valueOf(deviceSelectRatio));
    fields.add(String.valueOf(sensorSelectRatio));
    fields.add(String.valueOf(timeSelectRatio));
    fields.add(String.valueOf(queryNum));
    return String.join(",", fields);
  }
  
  public static String toStringHeader() {
    List<String> fields = new ArrayList<>();
    fields.add("useParallel");
    fields.add("fileNum");
    fields.add("deviceNum");
    fields.add("sensorNum");
    fields.add("ptNum");
    fields.add("dataType");
    fields.add("encoding");
    fields.add("deviceSelectRatio");
    fields.add("sensorSelectRatio");
    fields.add("timeSelectRatio");
    fields.add("queryNum");
    return String.join(",", fields);
  }
}
