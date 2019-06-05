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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecordUtils is a utility class for parsing data in form of CSV string.
 *
 * @author kangrong
 */
public class RecordUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

  private RecordUtils(){}

  /**
   * support input format: {@code <deviceId>,<timestamp>,[<measurementId>,<value>,]}.CSV line is
   * separated by ","
   *
   * @param str - input string
   * @param schema - constructed file schema
   * @return InsertPlan constructed from str
   */
  public static InsertPlan parseSimpleTuplePlan(String str, FileSchema schema) {
    // split items
    String[] items = str.split(JsonFormatConstant.TSRECORD_SEPARATOR);
    // get deviceId and timestamp, then create a new TSRecord
    String deviceId = items[0].trim();
    long timestamp;
    try {
      timestamp = Long.valueOf(items[1].trim());
    } catch (NumberFormatException e) {
      LOG.warn("given timestamp is illegal:{}", str);
      // return a TSRecord without any data points
      return new InsertPlan(deviceId, -1, null, null);
    }
    int valuesNum = (items.length - 2) / 2;
    String[] measurements = new String[valuesNum];
    String[] values = new String[valuesNum];
    int idx = 0;
    // loop all rest items except the last one
    String measurementId;
    TSDataType type;
    for (int i = 2; i < items.length - 1; i += 2) {
      // get measurementId and value
      measurementId = items[i].trim();
      String value = items[i + 1].trim();
      type = schema.getMeasurementDataType(measurementId);
      if (type == null) {
        LOG.warn("measurementId:{},type not found, pass", measurementId);
        continue;
      }
      measurements[idx] = measurementId;
      values[idx++] = value;
    }
    return new InsertPlan(deviceId, timestamp, measurements, values);
  }
}
