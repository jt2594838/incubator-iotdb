package org.apache.iotdb.tsfile.read;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public interface ITsFileReader {
  QueryDataSet query(QueryExpression queryExpression) throws IOException;

  void close() throws IOException;
}
