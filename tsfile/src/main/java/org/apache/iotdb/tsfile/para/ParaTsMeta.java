package org.apache.iotdb.tsfile.para;

import java.util.List;

public class ParaTsMeta {

  public ParaTsMeta(List<String> fileList, HashFunc hashFunc) {
    this.fileList = fileList;
    this.hashFunc = hashFunc;
  }

  /**
   * the paths of TsFiles making up this ParaTsFileWriter
   */
  private List<String> fileList;

  /**
   * the hash function used to map a timeseries to a file
   */
  private HashFunc hashFunc;

  public List<String> getFileList() {
    return fileList;
  }

  public HashFunc getHashFunc() {
    return hashFunc;
  }
}
