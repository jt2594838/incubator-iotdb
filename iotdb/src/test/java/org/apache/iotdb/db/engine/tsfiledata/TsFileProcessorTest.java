/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.tsfiledata;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.datasource.SeriesDataSource;
import org.apache.iotdb.db.engine.sgmanager.OperationResult;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessorTest {
  private static Logger LOGGER = LoggerFactory.getLogger(TsFileProcessorTest.class);
  protected TsFileProcessor processor;
  private MManager mManager;
  private Map<String, MeasurementSchema> measurementSchemaMap = new HashMap<>();

  protected FileSchema schema;

  private long oldTsFileSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getBufferwriteFileSizeThreshold();
  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
//  now we do not support wal because it need to modify the wal module.
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(true);
    IoTDBDescriptor.getInstance().getConfig().setBufferwriteFileSizeThreshold(2*1024*1024);
    mManager = MManager.getInstance();
    measurementSchemaMap.put("s1", new MeasurementSchema("s1", TSDataType.FLOAT,
        TSEncoding.RLE));
    measurementSchemaMap.put("s2", new MeasurementSchema("s2", TSDataType.FLOAT,
        TSEncoding.RLE));
    measurementSchemaMap.put("s3", new MeasurementSchema("s3", TSDataType.FLOAT,
        TSEncoding.RLE));
    schema = new FileSchema(measurementSchemaMap);
    processor = new TsFileProcessor("root.test", SysTimeVersionController.INSTANCE,
        schema);
    mManager.setStorageLevelToMTree("root.test");
    mManager.addPathToMTree("root.test.d1.s1",  TSDataType.FLOAT, TSEncoding.RLE,
        CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d2.s1",  TSDataType.FLOAT, TSEncoding.RLE,
        CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d1.s2",  TSDataType.FLOAT, TSEncoding.RLE,
        CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d2.s2",  TSDataType.FLOAT, TSEncoding.RLE,
        CompressionType.SNAPPY, Collections.emptyMap());
  }


  @After
  public void tearDown() throws Exception {
    //processor.close();
    // processor.writeLock();
    processor.removeMe();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(false);
    IoTDBDescriptor.getInstance().getConfig().
        setBufferwriteFileSizeThreshold(oldTsFileSizeThreshold);
  }

  @Test
  public void insert()
      throws IOException, ExecutionException, InterruptedException, TsFileProcessorException {
    String[] s1 = new String[]{"s1"};
    String[] s2 = new String[]{"s2"};
    String[] value = new String[]{"5.0"};

    assertEquals(
        OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1",  10, s1, value)));
    assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1",  10, s2, value)));
    assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1",  12, s1, value)));
    Future<Boolean> ok = processor.flush();
    ok.get();
    ok = processor.flush();
    Assert.assertTrue(ok instanceof ImmediateFuture);
    ok.get();
    ok = processor.flush();
    Assert.assertTrue(ok instanceof ImmediateFuture);
    ok.get();

    //let's rewrite timestamp =12 again..
    assertEquals(OperationResult.WRITE_REJECT_BY_TIME,
        processor.insert(new InsertPlan("root.test.d1",  12, s1, value)));
    processor.delete("root.test.d1", "s1",12);
    assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1",  12, s1, value)));
    assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1",  13, s1, value)));
    assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d2",  10, s1, value)));
    assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1",  14, s1, value)));
    processor.delete("root.test.d1", "s1",12);
    processor.delete("root.test.d3", "s1",12);

    executeQuery("root.test.d1", "s1", TEST_QUERY_CONTEXT, 2);
  }

  private void executeQuery(String deviceId, String measurementId, QueryContext context,
      int expectedCnt)
      throws IOException {
    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(
        new Path(deviceId, measurementId), null);
    SeriesDataSource dataSource = processor.query(singleSeriesExpression, context);
    SequenceDataReader dataReader = new SequenceDataReader(dataSource, null, context);
    int i = 0;
    while (dataReader.hasNext()) {
      BatchData batch = dataReader.nextBatch();
      while (batch.hasNext()) {
        //System.out.println(batch.currentTime() +"," + batch.getFloat());
        batch.next();
        i ++;
      }
    }
    if (expectedCnt >= 0) {
      assertEquals(expectedCnt, i);
    }
  }



  @Test
  public void bruteForceTest() throws InterruptedException, IOException {

    final boolean[] exception = {false, false, false};
    final boolean[] goon = {true};
    int totalSize = 50000;

    final int[] count = {0};
    Thread insertThread = new Thread(() -> {
      long time = 100L;
      long start = System.currentTimeMillis();
      String[] sensors = new String[]{"s1"};
      String[] values = new String[1];
      try {
        for (int j = 0; j < totalSize  && goon[0]; j++) {
          processor.lock(true);
//            processor.insert("root.test.d1","s1", time++,  String.valueOf(j));
//            processor.insert("root.test.d2","s1", time++,  String.valueOf(j));
          values[0] = String.valueOf(j);
          processor.insert(new InsertPlan("root.test.d1",  time++, sensors, values));
          processor.insert(new InsertPlan("root.test.d2",  time++, sensors, values));
          processor.writeUnlock();
          count[0]++;
        }
        System.out.println((System.currentTimeMillis() - start));
      } catch (TsFileProcessorException e) {
        LOGGER.error("", e);
        exception[0] = true;
      }
    });
    Thread flushThread = new Thread(() -> {
      try {
        for (int j = 0; j < totalSize * 2 && goon[0]; j++) {
          processor.lock(true);
          processor.flush();
          processor.writeUnlock();
        }
      } catch (IOException e) {
        // we will break out.
        LOGGER.error(e.getMessage());
        exception[1] = true;
      }
    });
    //we temporary disable the query because there are bugs..
    Thread queryThread = new Thread(() -> {
      try {
        for (int j = 0; j < totalSize * 2 && goon[0]; j++) {
          processor.lock(false);
          QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());
          executeQuery("root.test.d1", "s1", context, -1);
          QueryResourceManager.getInstance().endQueryForGivenJob(context.getJobId());
          processor.readUnlock();
        }
      } catch (IOException | StorageGroupManagerException e) {
        // we will break out.
        LOGGER.error(e.getMessage());
        exception[2] = true;
      }
    });
    flushThread.start();
    insertThread.start();
    queryThread.start();
    //wait at most 20 seconds.
    insertThread.join(20000);
    goon[0] = false;
    //queryThread.join(5000);
    Assert.assertFalse(exception[0]);
    Assert.assertFalse(exception[1]);
    Assert.assertFalse(exception[2]);
    synchronized (this) {
      while (queryThread.isAlive()) {
        this.wait(50);
      }
    }
    executeQuery("root.test.d1", "s1", TEST_QUERY_CONTEXT, 50000);
  }
}


