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
package org.apache.iotdb.db.engine.memcontrol;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.PathUtils;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileSchemaUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileSizeControlTest {

  private TsFileProcessor processor = null;
  private String nsp = "root.vehicle.d0";
  private String nsp2 = "root.vehicle.d1";

  private int groupSizeInByte;
  private int pageCheckSizeThreshold;
  private int pageSizeInByte;
  private int maxStringLength;
  private long fileSizeThreshold;
  private long memMonitorInterval;
  private IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();

  private boolean skip = !false;

  @Before
  public void setUp() throws Exception {
    // origin value
    groupSizeInByte = TSFileConfig.groupSizeInByte;
    pageCheckSizeThreshold = TSFileConfig.pageCheckSizeThreshold;
    pageSizeInByte = TSFileConfig.pageSizeInByte;
    maxStringLength = TSFileConfig.maxStringLength;
    fileSizeThreshold = dbConfig.getBufferwriteFileSizeThreshold();
    memMonitorInterval = dbConfig.getMemMonitorInterval();
    // new value
    TSFileConfig.groupSizeInByte = 200000;
    TSFileConfig.pageCheckSizeThreshold = 3;
    TSFileConfig.pageSizeInByte = 10000;
    TSFileConfig.maxStringLength = 2;
    dbConfig.setBufferwriteFileSizeThreshold(5 * 1024 * 1024);
    BasicMemController.getInstance().setCheckInterval(600 * 1000);
    // init metadata
    MetadataManagerHelper.initMetadata();
  }

  @After
  public void tearDown() throws Exception {
    // recovery value
    TSFileConfig.groupSizeInByte = groupSizeInByte;
    TSFileConfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
    TSFileConfig.pageSizeInByte = pageSizeInByte;
    TSFileConfig.maxStringLength = maxStringLength;
    dbConfig.setBufferwriteFileSizeThreshold(fileSizeThreshold);
    BasicMemController.getInstance().setCheckInterval(memMonitorInterval);
    // clean environment
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws WriteProcessException, TsFileProcessorException {
    if (skip) {
      return;
    }
    String filename = "bufferwritetest";
    //noinspection ResultOfMethodCallIgnored
    new File(filename).delete();

    try {
      processor = new TsFileProcessor(nsp, SysTimeVersionController.INSTANCE,
          FileSchemaUtils.constructFileSchema(nsp));
    } catch (TsFileProcessorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    File nspdir = PathUtils.getBufferWriteDir(nsp);
    assertTrue(nspdir.isDirectory());
    for (int i = 0; i < 1000000; i++) {
      processor.insert(new InsertPlan(nsp,i * i, "s1", i + ""));
      processor.insert(new InsertPlan(nsp2, i * i, "s1", i + ""));
      if (i % 100000 == 0) {
        System.out.println(i + "," + MemUtils.bytesCntToStr(processor.currentFileSize()));
      }
    }
    // wait to flush end
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    processor.close();
    assertTrue(processor.currentFileSize() < dbConfig.getBufferwriteFileSizeThreshold());
    fail("Method unimplemented");
  }
}
