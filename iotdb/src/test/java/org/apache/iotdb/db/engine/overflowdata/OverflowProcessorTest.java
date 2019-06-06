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

package org.apache.iotdb.db.engine.overflowdata;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.cleanEnv;
import static org.apache.iotdb.db.utils.EnvironmentUtils.envSetUp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.datasource.SeriesDataSource;
import org.apache.iotdb.db.engine.sgmanager.OperationResult;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessorTest;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OverflowProcessorTest extends TsFileProcessorTest {
  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(true);
    super.setUp();
    processor.close();
    processor = new OverflowProcessor("root.test", SysTimeVersionController.INSTANCE,
        schema);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    cleanEnv();
  }

  @Test
  public void insert()
      throws IOException, ExecutionException, InterruptedException,
      TsFileProcessorException {
    String[] s1 = new String[]{"s1"};
    String[] s2 = new String[]{"s2"};
    String[] value = new String[]{"5.0"};

    Assert.assertEquals(
        OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 12, s1, value)));
    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 11, s2, value)));
    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 10, s1, value)));
    Future<Boolean> ok = processor.flush();
    ok.get();
    ok = processor.flush();
    Assert.assertTrue(ok instanceof ImmediateFuture);
    ok.get();
    ok = processor.flush();
    Assert.assertTrue(ok instanceof ImmediateFuture);
    ok.get();

    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 10, s1, value)));
    processor.delete("root.test.d1", "s1", 11);
    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 10, s1, value)));
    ok = processor.flush();
    ok.get();
    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 10, s1, value)));
    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d2", 8, s1, value)));
    Assert.assertEquals(OperationResult.WRITE_SUCCESS,
        processor.insert(new InsertPlan("root.test.d1", 7, s1, value)));
    processor.delete("root.test.d1", "s1", 8);
    processor.delete("root.test.d3", "s1", 8);

    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(
        new Path("root.test.d1", "s1"), null);
    SeriesDataSource dataSource = processor.query(singleSeriesExpression, TEST_QUERY_CONTEXT);
    SequenceDataReader dataReader = new SequenceDataReader(dataSource, null, TEST_QUERY_CONTEXT);
    while (dataReader.hasNext()) {
      BatchData batch = dataReader.nextBatch();
      while (batch.hasNext()) {
        System.out.println(batch.currentTime() +"," + batch.getFloat());
        batch.next();
      }
    }
  }

}
