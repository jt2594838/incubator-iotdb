package org.apache.iotdb.tsfile.para;

@FunctionalInterface
public interface HashFunc {

  int hash(String str);

  class BasicHash implements HashFunc{

    @Override
    public int hash(String str) {
      return str.hashCode();
    }
  }
}
