package pl.rtshadow.lem.benchmarks;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static pl.rtshadow.lem.benchmarks.HdfsUtilities.*;

public class SimpleHdfsWriteReads {
  private final TestHdfsCluster testHdfsCluster = new TestHdfsCluster();
  private FileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    testHdfsCluster.start();
    fileSystem = testHdfsCluster.getFileSystem();
  }

  @Test
  public void readsAndWritesSimpleFile() throws IOException {
    Path path = new Path("test_file");
    String fileContent = "content";

    writeFile(fileSystem, path, fileContent);

    assertThat(readWholeFile(fileSystem, path)).isEqualTo(fileContent);
  }

  @Test
  public void appendWorks() throws IOException {
    Path path = new Path("test_file");
    String basicContent = "content";
    String appendedContent = "appended";

    writeFile(fileSystem, path, basicContent);
    appendFile(fileSystem, path, appendedContent);

    assertThat(readWholeFile(fileSystem, path)).isEqualTo(basicContent + appendedContent);
  }

  @After
  public void tearDown() {
    testHdfsCluster.stop();
  }
}
