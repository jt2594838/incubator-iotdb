package org.apache.iotdb.tsfile.para.benchmark;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.para.ParaTsFileReader;
import org.apache.iotdb.tsfile.para.ParaTsFileWriter;
import org.apache.iotdb.tsfile.para.ParaTsMeta;
import org.apache.iotdb.tsfile.read.ITsFileReader;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.ITsFileWriter;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class ParaBenchMark {

  private long writeTime;
  private long readTime;
  private long totalPts;

  public void execute(BenchMarkConfig config) throws IOException, WriteProcessException {
    if (config.isUseParallel()) {
      executeParallel(config);
    } else {
      executeNonParallel(config);
    }
    totalPts = config.getDeviceNum() * config.getSensorNum() * config.getPtNum();
    for (String filePath : config.getFilePaths()) {
      new File(filePath).delete();
    }
  }

  private void executeParallel(BenchMarkConfig config) throws IOException, WriteProcessException {
    executeParallelWrite(config);
    executeParallelRead(config);
  }

  private void executeParallelWrite(BenchMarkConfig config)
      throws IOException, WriteProcessException {
    LoadGenerator generator = new PrefetchLoadGenerator(new UniformLoadGenerator(config));
    List<String> paths = Arrays.asList(config.getFilePaths());
    ParaTsMeta meta = new ParaTsMeta(paths, config.getHashFunc());
    ParaTsFileWriter writer = new ParaTsFileWriter(meta);
    registerMeta(config, writer);
    executeWrite(generator, writer);
  }

  private void executeParallelRead(BenchMarkConfig config) throws IOException {
    QueryGenerator generator = new PrefetchQueryGenerator(new RandomQueryGenerator(config));
    List<String> paths = Arrays.asList(config.getFilePaths());
    ParaTsMeta meta = new ParaTsMeta(paths, config.getHashFunc());
    ParaTsFileReader reader = new ParaTsFileReader(meta);
    executeRead(generator, reader);
  }

  private void executeNonParallel(BenchMarkConfig config)
      throws IOException, WriteProcessException {
    executeNonParallelWrite(config);
    executeNonParallelRead(config);
  }

  private void executeNonParallelWrite(BenchMarkConfig config)
      throws IOException, WriteProcessException {
    LoadGenerator generator = new PrefetchLoadGenerator(new UniformLoadGenerator(config));
    String path = config.getFilePaths()[0];
    TsFileWriter writer = new TsFileWriter(new File(path));
    registerMeta(config, writer);
    executeWrite(generator, writer);
  }

  private void executeNonParallelRead(BenchMarkConfig config) throws IOException {
    QueryGenerator generator = new PrefetchQueryGenerator(new RandomQueryGenerator(config));
    String path = config.getFilePaths()[0];
    ReadOnlyTsFile reader = new ReadOnlyTsFile(new TsFileSequenceReader(path));
    executeRead(generator, reader);
  }

  private void executeWrite(LoadGenerator generator, ITsFileWriter writer)
      throws IOException, WriteProcessException {
    long startTime = System.currentTimeMillis();
    while (generator.hasNext()) {
      writer.write(generator.next());
    }
    System.out.println("write used " + (System.currentTimeMillis() - startTime));
    writer.close();
    System.out.println("writer close used " + (System.currentTimeMillis() - startTime));
    writeTime = System.currentTimeMillis() - startTime;
  }

  private void executeRead(QueryGenerator generator, ITsFileReader reader) throws IOException {
    long startTime = System.currentTimeMillis();
    int cnt = 0;
    while (generator.hasNext()) {
      QueryDataSet dataSet = reader.query(generator.next());
      while (dataSet.hasNext()) {
        dataSet.next();
        cnt ++;
      }
      dataSet.close();
    }
    System.out.println("query used " + (System.currentTimeMillis() - startTime));
    System.out.println("Query result: " + cnt);
    reader.close();
    System.out.println("reader close used " + (System.currentTimeMillis() - startTime));
    readTime = System.currentTimeMillis() - startTime;
  }

  private void registerMeta(BenchMarkConfig config, ITsFileWriter writer)
      throws WriteProcessException {
    for (int i = 0; i < config.getSensorNum(); i++) {
      writer.addMeasurement(new MeasurementSchema("sensor" + i, config.getDataType(),
          config.getEncoding()));
    }
  }

  public String genReport(BenchMarkConfig config) {
    return config.toString() + "," + writeTime + "," + readTime;
  }

  public String genReportHeader() {
    return BenchMarkConfig.toStringHeader() + ",writeTime,readTime";
  }

  private static void setFilePaths(BenchMarkConfig config, int[] fileNums, String[] parentDirs) {
    List<String> filePaths = new ArrayList<>();
    for (int i = 0; i < parentDirs.length; i++) {
      for (int j = 0; j < fileNums[i]; j++) {
        filePaths.add(parentDirs[i] + File.separator + "tsfile" + i);
      }
    }
    config.setFilePaths(filePaths.toArray(new String[0]));
  }

  /**
   * example:
   *     ParaBenchMark benchMark = new ParaBenchMark();
   *     BenchMarkConfig config = new BenchMarkConfig();
   *
   *     config.setUseParallel(true);
   *     config.setDeviceSelectRatio(0.5);
   *     config.setSensorSelectRatio(0.5);
   *     config.setTimeSelectRatio(0.5);
   *     config.setQueryNum(10);
   *     config.setFilePaths(new String[] {
   *         "file1", "file2", "file3", "file4"
   *     });
   *
   *     benchMark.execute(config);
   *     System.out.println(benchMark.genReportHeader());
   *     System.out.println(benchMark.genReport(config));
   */
  public static void main(String[] args) throws IOException, WriteProcessException {
    ParaBenchMark benchMark = new ParaBenchMark();
    BenchMarkConfig config = new BenchMarkConfig();
    String reportPath = "report.csv";
    String[] parentDirs = new String[] {
        ""
    };
    config.setUseParallel(true);
    config.setDeviceSelectRatio(0.5);
    config.setSensorSelectRatio(0.5);
    config.setTimeSelectRatio(0.5);
    config.setQueryNum(10);
    int totalFileNum = 5;

    try (FileWriter writer = new FileWriter(reportPath)) {
      writer.write(benchMark.genReportHeader() + '\n');
      for (int fileNumInSSD = 0; fileNumInSSD <= totalFileNum; fileNumInSSD++) {
        int[] fileNums = new int[] {
            totalFileNum - fileNumInSSD, fileNumInSSD
        };
        setFilePaths(config, fileNums, parentDirs);
        benchMark.execute(config);
        writer.write(benchMark.genReport(config) + '\n');
      }
      config.setUseParallel(false);
      // single file in hdd
      setFilePaths(config, new int[]{1, 0}, parentDirs);
      benchMark.execute(config);
      writer.write(benchMark.genReport(config) + '\n');
      // single file in ssd
      setFilePaths(config, new int[]{0, 1}, parentDirs);
      benchMark.execute(config);
      writer.write(benchMark.genReport(config) + '\n');
    }
  }
}
