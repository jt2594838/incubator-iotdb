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
package org.apache.iotdb.tsfile.read.query.dataset;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class QueryDataSet {

  protected List<Path> paths;
  protected List<TSDataType> dataTypes;

  public QueryDataSet() {

  }

  public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes) {
    this.paths = paths;
    this.dataTypes = dataTypes;
  }

  public QueryDataSet(List<Path> paths) {
    this.paths = paths;
  }

  /**
   * This method is used for batch query.
   */
  public abstract boolean hasNext() throws IOException;

  /**
   * This method is used for batch query, return RowRecord.
   */
  public abstract RowRecord next() throws IOException;

  public List<Path> getPaths() {
    return paths;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public void close() {}

  protected Field getField(Object value, TSDataType dataType) {
    Field field = new Field(dataType);

    if (value == null) {
      field.setNull();
      return field;
    }

    switch (dataType) {
      case DOUBLE:
        field.setDoubleV((double) value);
        break;
      case FLOAT:
        field.setFloatV((float) value);
        break;
      case INT64:
        field.setLongV((long) value);
        break;
      case INT32:
        field.setIntV((int) value);
        break;
      case BOOLEAN:
        field.setBoolV((boolean) value);
        break;
      case TEXT:
        field.setBinaryV((Binary) value);
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported: " + dataType);
    }
    return field;
  }
}
