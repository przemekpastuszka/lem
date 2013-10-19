package pl.rtshadow.lem.benchmarks.benchmarks;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities;

import java.io.IOException;
import java.util.UUID;

import static pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities.writeFile;

public class FileCreationAndAppendBenchmark extends BenchmarkRunner {
  @Test
  public void test() throws Exception {
    performTest(prepareCase(), 3000, 1);
  }

  private BenchmarkCase prepareCase() throws IOException {
    final String dataToWrite = StringUtils.repeat('A', 1025);
    final Path fileName = new Path(UUID.randomUUID().toString());
    HdfsUtilities.writeFile(fileSystem, fileName, "");

    return new BenchmarkCase() {
      @Override
      public void run(FileSystem fileSystem) throws IOException {
        HdfsUtilities.appendFile(fileSystem, fileName, dataToWrite);
      }
    };
  }
}
