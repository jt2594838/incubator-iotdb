package org.apache.iotdb.tsfile.para.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;

public class RandomQueryGenerator implements QueryGenerator {

  private int deviceNum;
  private int sensorNum;
  private int timeLimit;

  private double deviceSelectRatio;
  private double sensorSelectRatio;
  private double timeSelectRatio;
  private int queryNum = 100;

  private int idx = 0;

  private Random random;

  public RandomQueryGenerator(BenchMarkConfig config) {
    this.deviceNum = config.getDeviceNum();
    this.sensorNum = config.getSensorNum();
    this.timeLimit = config.getPtNum();
    this.deviceSelectRatio = config.getDeviceSelectRatio();
    this.sensorSelectRatio = config.getSensorSelectRatio();
    this.timeSelectRatio = config.getTimeSelectRatio();
    this.random = new Random(System.currentTimeMillis());
  }

  @Override
  public boolean hasNext() {
    return idx < queryNum;
  }

  @Override
  public QueryExpression next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    int selectDeviceNum = (int) (deviceNum * deviceSelectRatio);
    int selectSensorNum = (int) (sensorNum * sensorSelectRatio);
    long timeBound = (long) (timeLimit * timeSelectRatio);
    List<Path> paths = new ArrayList<>();
    IntStream devices = random.ints(selectDeviceNum, 0, deviceNum);
    devices.forEach(deviceId -> {
      IntStream sensors = random.ints(selectSensorNum, 0, sensorNum);
      sensors.forEach(sensorId -> {
        paths.add(new Path("root.device" + deviceId, "sensor" + sensorId));
      });
    });

    IExpression timeExpression = new GlobalTimeExpression(TimeFilter.ltEq(timeBound));
    QueryExpression queryExpression = QueryExpression.create(paths, timeExpression);
    idx++;
    return queryExpression;
  }
}
