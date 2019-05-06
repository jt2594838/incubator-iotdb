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
package org.apache.iotdb.db.engine.memcontrol;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController.UsageLevel;
import org.apache.iotdb.db.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class only gives a hint to FilenodeManager that it may flush some data to avoid rush hour.
 */
public class ClosePartialPolicy implements Policy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosePartialPolicy.class);
  private Thread workerThread;
  private long sleepInterval = IoTDBDescriptor.getInstance().getConfig().getSmallFlushInterval();

  @Override
  public void execute() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Memory reaches {}, current memory size is {}, JVM memory is {}, closing.",
              BasicMemController.getInstance().getCurrLevel(),
              MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()),
              MemUtils.bytesCntToStr(Runtime.getRuntime().totalMemory()
                      - Runtime.getRuntime().freeMemory()));
    }

    // use a thread to avoid blocking
    if (workerThread == null) {
      workerThread = createWorkerThread();
      workerThread.start();
    } else {
      if (workerThread.isAlive()) {
        LOGGER.debug("Last close is ongoing...");
      } else {
        workerThread = createWorkerThread();
        workerThread.start();
      }
    }
  }

  private Thread createWorkerThread() {
    return new Thread(() -> {
      FileNodeManager.getInstance().forceClose(UsageLevel.WARNING);
      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOGGER.warn("Flush worker interrupted!", e);
        Thread.currentThread().interrupt();
      }
    }, ThreadName.FLUSH_PARTIAL_POLICY.getName());
  }
}