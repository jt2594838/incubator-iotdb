package org.apache.iotdb.tsfile.para;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.read.ITsFileReader;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ParaTsFileReader implements ITsFileReader {

  private ParaTsMeta meta;
  private ReadOnlyTsFile[] readers;
  private ExecutorService pool;

  private boolean isClosed = false;

  public ParaTsFileReader(ParaTsMeta meta) throws IOException {
    this.meta = meta;
    initReaders();
    pool = Executors.newFixedThreadPool(meta.getFileList().size());
  }

  private void initReaders() throws IOException {
    readers = new ReadOnlyTsFile[meta.getFileList().size()];
    List<String> filePaths = meta.getFileList();
    for (int i = 0; i < filePaths.size(); i ++) {
      readers[i] = new ReadOnlyTsFile(new TsFileSequenceReader(filePaths.get(i)));
    }
  }

  public QueryDataSet query(QueryExpression queryExpression) throws IOException {
    if (isClosed) {
      throw new IOException("reader closed");
    }
    return new ParaQueryDataSet(queryExpression);
  }


  private QueryExpression[] splitQueryExpression(QueryExpression queryExpression) {
    // long startTime = System.currentTimeMillis();
    List<Path> paths = queryExpression.getSelectedSeries();
    IExpression expression = queryExpression.getExpression();
    QueryExpression[] ret = new QueryExpression[meta.getFileList().size()];
    List[] pathLists = new List[meta.getFileList().size()];
    // split paths by deviceId
    for (Path path : paths) {
      int hash = meta.getHashFunc().hash(path.getDevice());
      hash = hash < 0 ? -hash : hash;
      int idx = hash % pathLists.length;
      if (pathLists[idx] == null) {
        pathLists[idx] = new ArrayList();
      }
      pathLists[idx].add(path);
    }
    // create expressions for non-empty lists
    for (int i = 0; i < meta.getFileList().size(); i++) {
      if (pathLists[i] != null) {
        ret[i] = QueryExpression.create(pathLists[i], expression);
      }
    }
    // System.out.println("split query used " + (System.currentTimeMillis() - startTime));
    return ret;
  }

  public void close() throws IOException {
    for (ReadOnlyTsFile tsFile : readers) {
      tsFile.close();
    }
    pool.shutdown();
    isClosed = true;
  }

  class ParaQueryDataSet extends QueryDataSet {

    private List<ConcurrentLinkedQueue<RowRecord>> recordQueues = new ArrayList<>();
    private List<Future> readTasks = new ArrayList<>();
    private int preFetchNum = TSFileConfig.paraPreFetchNum;

    private ParaQueryDataSet(QueryExpression queryExpression) throws IOException {
      QueryExpression[] queryExpressions = splitQueryExpression(queryExpression);
      for (int i = 0; i < queryExpressions.length; i++) {
        if (queryExpressions[i] != null) {
          ConcurrentLinkedQueue<RowRecord> records = new ConcurrentLinkedQueue<>();
          recordQueues.add(records);
          readTasks.add(pool.submit(new ReadTask(queryExpressions[i], records, readers[i],
              preFetchNum)));
        }
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      for (int i = 0; i < readTasks.size(); i ++) {
        if (!readTasks.get(i).isDone() || !recordQueues.get(i).isEmpty()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public RowRecord next() throws IOException {
      waitForFetch();
      RowRecord minRecord = selectMinTime();
      mergeRecords(minRecord);

      return minRecord;
    }

    @Override
    public void close() {
      for (Future readTask : readTasks) {
        readTask.cancel(true);
      }
    }

    private void waitForFetch() {
      for (int i = 0; i < readTasks.size(); i ++) {
        while (recordQueues.get(i).isEmpty() && !readTasks.get(i).isDone()) {
          // wait until each unfinished readTasks fetch at least one Row
        }
      }
    }

    private RowRecord selectMinTime() {
      RowRecord minRecord = null;
      long minTime = Long.MAX_VALUE;
      // iterate each queue to find the minimum time stamp
      for (ConcurrentLinkedQueue<RowRecord> recordQueue1 : recordQueues) {
        if (!recordQueue1.isEmpty()) {
          RowRecord record = recordQueue1.peek();
          if (record.getTimestamp() < minTime) {
            minRecord = record;
            minTime = record.getTimestamp();
          }
        }
      }
      return minRecord;
    }

    private void mergeRecords(RowRecord minRecord) {
      // iterate again to pop and merge all rows with the minimum time stamp
      for (ConcurrentLinkedQueue<RowRecord> recordQueue : recordQueues) {
        if (!recordQueue.isEmpty()) {
          RowRecord record = recordQueue.poll();
          if (record.getTimestamp() == minRecord.getTimestamp() && record != minRecord) {
            minRecord.getFields().addAll(record.getFields());
          }
        }
      }
    }
  }

  class ReadTask implements Callable<Void> {

    private int fetchSize;
    private QueryDataSet dataSet;
    private ConcurrentLinkedQueue<RowRecord> queue;
    boolean stop = false;

    private ReadTask(QueryExpression queryExpression, ConcurrentLinkedQueue<RowRecord> queue,
        ReadOnlyTsFile readOnlyTsFile, int fetchSize) throws IOException {
      this.fetchSize = fetchSize;
      this.dataSet = readOnlyTsFile.query(queryExpression);
      this.queue = queue;
    }

    @Override
    public Void call() throws Exception {
      // long startTime = System.currentTimeMillis();
      while (!stop) {
//        if (queue.size() >= fetchSize) {
//          Thread.sleep(10);
//        }
        queue.add(dataSet.next());
        if (!dataSet.hasNext() || Thread.interrupted()) {
          stop = true;
        }
      }
      dataSet.close();
      // long endTime = System.currentTimeMillis();
      // System.out.println("one reader used " + (endTime - startTime));
      return null;
    }
  }
}
