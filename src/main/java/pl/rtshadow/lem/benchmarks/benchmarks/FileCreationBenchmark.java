package pl.rtshadow.lem.benchmarks.benchmarks;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities.writeFile;

public class FileCreationBenchmark extends BenchmarkRunner {
  @Test
  public void testFileCreationWithNoDataWritten() throws Exception {
    performTest(prepareCase(), 10, 1);
  }

  private BenchmarkCase prepareCase() {
    return new BenchmarkCase() {
      @Override
      public void run(FileSystem fileSystem) throws IOException {
        writeFile(fileSystem, new Path(UUID.randomUUID().toString()), "");
      }
    };
  }
}
