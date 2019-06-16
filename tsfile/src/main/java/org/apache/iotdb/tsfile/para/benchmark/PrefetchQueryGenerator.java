package org.apache.iotdb.tsfile.para.benchmark;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;

public class PrefetchQueryGenerator implements QueryGenerator {

  private List<QueryExpression> queryExpressionList = new ArrayList<>();
  private int idx;

  public PrefetchQueryGenerator(QueryGenerator queryGenerator) {
    while (queryGenerator.hasNext()) {
      queryExpressionList.add(queryGenerator.next());
    }
  }

  @Override
  public boolean hasNext() {
    return idx < queryExpressionList.size();
  }

  @Override
  public QueryExpression next() {
    return queryExpressionList.get(idx++);
  }
}
