package pl.rtshadow.lem.benchmarks.benchmarks;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities.writeFile;

public class FileCreationAndWriteBenchmark extends BenchmarkRunner {
  @Test
  public void test() throws Exception {
    performTest(prepareCase(), 5000, 1);
  }

  private BenchmarkCase prepareCase() {
    final String dataToWrite = StringUtils.repeat('A', 1025);

    return new BenchmarkCase() {
      @Override
      public void run(FileSystem fileSystem) throws IOException {
        writeFile(fileSystem, new Path(UUID.randomUUID().toString()), dataToWrite);
      }
    };
  }
}
