package pl.rtshadow.lem.benchmarks.benchmarks;


import org.apache.hadoop.fs.FileSystem;

public interface BenchmarkCase {
  void run(FileSystem fileSystem) throws Exception;
}
