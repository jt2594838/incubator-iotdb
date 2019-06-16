package org.apache.iotdb.tsfile.para.benchmark;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.write.record.TSRecord;

public class PrefetchLoadGenerator implements LoadGenerator{

  private List<TSRecord> recordList = new ArrayList<>();
  private int idx = 0;

  public PrefetchLoadGenerator(LoadGenerator generator) {
    while (generator.hasNext()) {
      recordList.add(generator.next());
    }
  }

  @Override
  public boolean hasNext() {
    return idx < recordList.size();
  }

  @Override
  public TSRecord next() {
    return recordList.get(idx++);
  }
}
