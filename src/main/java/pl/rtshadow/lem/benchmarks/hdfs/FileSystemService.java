package pl.rtshadow.lem.benchmarks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

public class FileSystemService {
  public DistributedFileSystem getFileSystem(Configuration configuration) {
    try {
      return (DistributedFileSystem) DistributedFileSystem.get(configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
