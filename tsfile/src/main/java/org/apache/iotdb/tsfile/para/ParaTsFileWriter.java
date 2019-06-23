package org.apache.iotdb.tsfile.para;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.ITsFileWriter;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParaTsFileWriter implements ITsFileWriter {

  private static Logger logger = LoggerFactory.getLogger(ParaTsFileWriter.class);

  private ParaTsMeta meta;
  private TsFileWriter[] writers;

  private ConcurrentLinkedQueue<TSRecord>[] recordQueues;

  private ExecutorService pool;
  private Future[] writeFutures;
  private WriteConsumer[] writeConsumers;

  private boolean closed = false;
  private AtomicInteger queueRecordCnt = new AtomicInteger(0);

  public ParaTsFileWriter(ParaTsMeta meta) throws IOException {
    this.meta = meta;

    init(meta.getFileList());
  }

  private void init(List<String> filePaths) throws IOException {
    this.recordQueues = new ConcurrentLinkedQueue[meta.getFileList().size()];
    for (int i = 0; i < meta.getFileList().size(); i ++) {
      recordQueues[i] = new ConcurrentLinkedQueue<>();
    }
    initFiles(filePaths);
    initConsumers();
  }

  private void initFiles(List<String> filePaths) throws IOException {
    this.writers = new TsFileWriter[filePaths.size()];
    int i = 0;
    for (String path : filePaths) {
      writers[i++] = new TsFileWriter(new File(path));
    }
  }

  private void initConsumers() {
    pool = Executors.newFixedThreadPool(writers.length);
    writeFutures = new Future[writers.length];
    writeConsumers = new WriteConsumer[writers.length];
    for (int i = 0; i < writers.length; i ++) {
      writeConsumers[i] = new WriteConsumer(recordQueues[i], writers[i]);
      writeFutures[i] = pool.submit(writeConsumers[i]);
    }
  }

  /**
   * add a measurementSchema to this ParaTsFile.
   */
  public void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException {
    for (TsFileWriter writer : writers) {
      writer.addMeasurement(measurementSchema);
    }
  }

  /**
   * write a record into the queue.
   *
   * @param record - record responding a data line
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  public void write(TSRecord record) throws IOException, WriteProcessException {
    if (closed) {
      throw new IOException("Writer closed");
    }
    int hash = meta.getHashFunc().hash(record.deviceId);
    hash = hash < 0 ? -hash : hash;
    int idx = hash % writers.length;

    while (queueRecordCnt.get() > 1000) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    recordQueues[idx].add(record);
    queueRecordCnt.incrementAndGet();
  }

  /**
   * canStop receiving any new recordQueues and close lower writers
   */
  public void close() throws IOException {
    boolean exceptionThrows = false;
    for (WriteConsumer writeConsumer : writeConsumers) {
      writeConsumer.stop();
    }
    for (Future future : writeFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        exceptionThrows = true;
        logger.error("Error in closing consumers, ", e);
      }
    }

    List<Future> closeFutures = new ArrayList<>();
    for (TsFileWriter writer : writers) {
      closeFutures.add(pool.submit(new CloseTask(writer)));
    }
    for (Future future : closeFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        exceptionThrows = true;
        logger.error("Error in close TsFileWriters, ", e);
      }
    }
    if (exceptionThrows) {
      throw new IOException("Cannot close all writers, see logs above for details");
    }
    pool.shutdown();
    closed = true;
  }


  class WriteConsumer implements Runnable {

    private boolean canStop = false;
    private ConcurrentLinkedQueue<TSRecord> records;
    private TsFileWriter tsFileWriter;

    private WriteConsumer(
        ConcurrentLinkedQueue<TSRecord> records,
        TsFileWriter tsFileWriter) {
      this.records = records;
      this.tsFileWriter = tsFileWriter;
    }

    @Override
    public void run() {
      boolean stop = false;
      while (!stop) {
        TSRecord record = records.poll();
        if (record == null) {
          if (canStop) {
            stop = true;
          }
          continue;
        }
        try {
          tsFileWriter.write(record);
          queueRecordCnt.getAndDecrement();
        } catch (IOException | WriteProcessException e) {
          logger.error("Cannot write record {}", record, e);
        }
        if (Thread.interrupted()) {
          stop();
        }
      }
    }

    private void stop() {
      this.canStop = true;
    }
  }

  class CloseTask implements Callable<Void> {

    private TsFileWriter writer;

    private CloseTask(TsFileWriter writer) {
      this.writer = writer;
    }

    @Override
    public Void call() throws Exception {
      try {
        writer.close();
      } catch (IOException e) {
        throw new IOException(String.format("Cannot close TsFile %s", writer.hashCode()), e);
      }
      return null;
    }
  }
}
