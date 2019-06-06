/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.DatabaseEngine;
import org.apache.iotdb.db.engine.DatabaseEngineFactory;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.MonitorConstants.FileSizeConstants;
import org.apache.iotdb.db.monitor.collector.FileSize;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MonitorTest {

  private IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
  private StatMonitor statMonitor;

  @Before
  public void setUp() throws Exception {
    // origin value
    // modify stat parameter
    EnvironmentUtils.closeMemControl();
    EnvironmentUtils.envSetUp();
    ioTDBConfig.setEnableStatMonitor(true);
    ioTDBConfig.setBackLoopPeriodSec(1);
  }

  @After
  public void tearDown() throws Exception {
    ioTDBConfig.setEnableStatMonitor(false);
    statMonitor.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDatabaseEngineMonitorAndAddMetadata() {
    DatabaseEngine dbEngine = DatabaseEngineFactory.getCurrent();
    FileSize fileSize = FileSize.getInstance();
    statMonitor = StatMonitor.getInstance();
    statMonitor.registerStatStorageGroup();
    dbEngine.getStatParamsHashMap().forEach((key, value) -> value.set(0));
    fileSize.getStatParamsHashMap().forEach((key, value) -> value.set(0));
    statMonitor.clearIStatisticMap();
    statMonitor.registerStatistics(dbEngine.getClass().getSimpleName(), dbEngine);
    statMonitor
        .registerStatistics(MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME, FileSize.getInstance());
    // add metadata
    MManager mManager = MManager.getInstance();
    dbEngine.registerStatMetadata();
    fileSize.registerStatMetadata();
    Map<String, AtomicLong> statParamsHashMap = dbEngine.getStatParamsHashMap();
    Map<String, AtomicLong> fileSizeStatsHashMap = fileSize.getStatParamsHashMap();
    for (String statParam : statParamsHashMap.keySet()) {
      assertTrue(mManager.pathExist(
          MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPARATOR
              + MonitorConstants.FILE_NODE_MANAGER_PATH + MonitorConstants.MONITOR_PATH_SEPARATOR
              + statParam));
    }
    for (String statParam : fileSizeStatsHashMap.keySet()) {
      assertTrue(mManager.pathExist(
          MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
              + statParam));
    }
    statMonitor.activate();
    // wait for time second
    try {
      Thread.sleep(5000);
      statMonitor.close();
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Get stat data and test right

    Map<String, InsertPlan> statHashMap = dbEngine.getAllStatisticsValue();
    Map<String, InsertPlan> fileSizeStatMap = fileSize.getAllStatisticsValue();

    String path = dbEngine.getAllPathForStatistic().get(0);
    String fileSizeStatPath = fileSize.getAllPathForStatistic().get(0);
    int pos = path.lastIndexOf('.');
    int fileSizeStatPos = fileSizeStatPath.lastIndexOf('.');
    InsertPlan fPlan = statHashMap.get(path.substring(0, pos));
    InsertPlan fileSizePlan = fileSizeStatMap.get(fileSizeStatPath.substring(0, fileSizeStatPos));

    assertNotEquals(null, fPlan);
    assertNotEquals(null, fileSizePlan);
    for (int i = 0; i < fPlan.getMeasurements().length; i++) {
      String m = fPlan.getMeasurements()[i];
      Long v = Long.parseLong(fPlan.getValues()[i]);
      if (m.equals("TOTAL_REQ_SUCCESS")) {
        assertEquals(v, new Long(0));
      }
      if (m.contains("FAIL")) {
        assertEquals(v, new Long(0));
      } else if (m.contains("POINTS")) {
        assertEquals(v, new Long(0));
      } else {
        assertEquals(v, new Long(0));
      }
    }
    for (int i = 0; i < fileSizePlan.getMeasurements().length; i++) {
      String m = fileSizePlan.getMeasurements()[i];
      Long v = Long.parseLong(fileSizePlan.getValues()[i]);
      if (m.equals(FileSizeConstants.OVERFLOW.name())) {
        assertEquals(v, new Long(0));
      }
    }

    try {
      dbEngine.deleteAll();
    } catch (StorageGroupManagerException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
