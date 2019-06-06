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

package org.apache.iotdb.db.engine.modification;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import junit.framework.TestCase;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.DatabaseEngineFactory;
import org.apache.iotdb.db.engine.datasource.QueryDataSource;
import org.apache.iotdb.db.engine.modification.io.LocalTextModificationAccessor;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeletionFileNodeTest {

  private String processorName = "root.test";

  private static String[] measurements = new String[10];
  private String dataType = TSDataType.DOUBLE.toString();
  private String encoding = TSEncoding.PLAIN.toString();

  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  @Before
  public void setup() throws
      PathErrorException, IOException, StorageGroupManagerException, StartupException {
    EnvironmentUtils.envSetUp();

    MManager.getInstance().setStorageLevelToMTree(processorName);
    for (int i = 0; i < 10; i++) {
      MManager.getInstance().addPathToMTree(processorName + "." + measurements[i], dataType,
          encoding);
      DatabaseEngineFactory.getCurrent()
          .addTimeSeries(new Path(processorName, measurements[i]), TSDataType.valueOf(dataType),
              TSEncoding.valueOf(encoding), CompressionType.valueOf(TSFileConfig.compressor),
              Collections.emptyMap());
    }
  }

  @After
  public void teardown() throws IOException, StorageGroupManagerException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeleteInBufferWriteCache() throws
      StorageGroupManagerException {

    for (int i = 1; i <= 100; i++) {
      String[] values = new String[measurements.length];
      for (int j = 0; j < measurements.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      InsertPlan plan = new InsertPlan(processorName, i, measurements, values);
      DatabaseEngineFactory.getCurrent().insert(plan, false);
    }

    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[3], 50);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[4], 50);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[5], 30);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[5], 50);

    SingleSeriesExpression expression = new SingleSeriesExpression(new Path(processorName,
        measurements[5]), null);
    QueryResourceManager.getInstance().beginQueryOfGivenExpression(TEST_QUERY_JOB_ID, expression);
    QueryDataSource dataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(expression.getSeriesPath(), TEST_QUERY_CONTEXT);

    Iterator<TimeValuePair> timeValuePairs =
        dataSource.getSeqDataSource().getReadableChunk().getIterator();
    int count = 0;
    while (timeValuePairs.hasNext()) {
      timeValuePairs.next();
      count++;
    }
    assertEquals(50, count);
    QueryResourceManager.getInstance().endQueryForGivenJob(TEST_QUERY_JOB_ID);
  }

  @Test
  public void testDeleteInBufferWriteFile() throws StorageGroupManagerException, IOException {
    for (int i = 1; i <= 100; i++) {
      String[] values = new String[measurements.length];
      for (int j = 0; j < measurements.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      InsertPlan plan = new InsertPlan(processorName, i, measurements, values);
      DatabaseEngineFactory.getCurrent().insert(plan, false);
    }
    DatabaseEngineFactory.getCurrent().closeAll();

    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[5], 50);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[4], 40);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[3], 30);

    Modification[] realModifications = new Modification[]{
        new Deletion(processorName + "." + measurements[5], 103, 50),
        new Deletion(processorName + "." + measurements[4], 105, 40),
        new Deletion(processorName + "." + measurements[3], 107, 30),
    };

    String fileNodePath = Directories.getInstance().getTsFileFolder(0) + File.separator
        + processorName;
    File fileNodeDir = new File(fileNodePath);
    File[] modFiles = fileNodeDir.listFiles((dir, name)
        -> name.endsWith(ModificationFile.FILE_SUFFIX));
    assertEquals(1, modFiles.length);

    LocalTextModificationAccessor accessor =
        new LocalTextModificationAccessor(modFiles[0].getPath());
    try {
      Collection<Modification> modifications = accessor.read();
      assertEquals( 3, modifications.size());
      int i = 0;
      for (Modification modification : modifications) {
        assertEquals(realModifications[i++], modification);
      }
    } finally {
      accessor.close();
    }
  }

  @Test
  public void testDeleteInOverflowCache() throws StorageGroupManagerException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      String[] values = new String[measurements.length];
      for (int j = 0; j < measurements.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      InsertPlan plan = new InsertPlan(processorName, i, measurements, values);
      DatabaseEngineFactory.getCurrent().insert(plan, false);
    }
    DatabaseEngineFactory.getCurrent().closeAll();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      String[] values = new String[measurements.length];
      for (int j = 0; j < measurements.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      InsertPlan plan = new InsertPlan(processorName, i, measurements, values);
      DatabaseEngineFactory.getCurrent().insert(plan, false);
    }

    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[3], 50);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[4], 50);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[5], 30);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[5], 50);

    SingleSeriesExpression expression = new SingleSeriesExpression(new Path(processorName,
        measurements[5]), null);

    QueryResourceManager.getInstance().beginQueryOfGivenExpression(TEST_QUERY_JOB_ID, expression);
    QueryDataSource dataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(expression.getSeriesPath(), TEST_QUERY_CONTEXT);

    Iterator<TimeValuePair> timeValuePairs =
        dataSource.getOverflowSeriesDataSource().getReadableChunk().getIterator();
    int count = 0;
    while (timeValuePairs.hasNext()) {
      timeValuePairs.next();
      count++;
    }
    assertEquals(50, count);

    QueryResourceManager.getInstance().endQueryForGivenJob(TEST_QUERY_JOB_ID);
  }

  @Test
  public void testDeleteInOverflowFile() throws StorageGroupManagerException, IOException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      String[] values = new String[measurements.length];
      for (int j = 0; j < measurements.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      InsertPlan plan = new InsertPlan(processorName, i, measurements, values);
      DatabaseEngineFactory.getCurrent().insert(plan, false);
    }
    DatabaseEngineFactory.getCurrent().closeAll();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      String[] values = new String[measurements.length];
      for (int j = 0; j < measurements.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      InsertPlan plan = new InsertPlan(processorName, i, measurements, values);
      DatabaseEngineFactory.getCurrent().insert(plan, false);
    }
    DatabaseEngineFactory.getCurrent().closeAll();

    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[5], 50);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[4], 40);
    DatabaseEngineFactory.getCurrent().deleteData(processorName, measurements[3], 30);

    Modification[] realModifications = new Modification[]{
        new Deletion(processorName + "." + measurements[5], 103, 50),
        new Deletion(processorName + "." + measurements[4], 105, 40),
        new Deletion(processorName + "." + measurements[3], 107, 30),
    };

    String fileNodePath = IoTDBDescriptor.getInstance().getConfig().getOverflowDataDirs()[0] +
        File.separator + processorName  + File.separator;
    File fileNodeDir = new File(fileNodePath);
    File[] modFiles = fileNodeDir.listFiles((dir, name)
        -> name.endsWith(ModificationFile.FILE_SUFFIX));
    assertEquals(1, modFiles.length);

    LocalTextModificationAccessor accessor =
        new LocalTextModificationAccessor(modFiles[0].getPath());
    Collection<Modification> modifications = accessor.read();
    assertEquals(3, modifications.size());
    int i = 0;
    for (Modification modification : modifications) {
      TestCase.assertEquals(realModifications[i++], modification);
    }
  }
}
