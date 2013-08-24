package pl.rtshadow.lem.benchmarks.tests;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.rtshadow.lem.benchmarks.contexts.AbstractTestWithContext;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;
import static pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities.*;

public class SimpleHdfsWriteReads extends AbstractTestWithContext {
  private final static Path TEST_FILE_PATH = new Path("test_file");

  private FileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    fileSystem = getInstance(FileSystem.class);
  }

  @Test
  public void readsAndWritesSimpleFile() throws IOException {
    String fileContent = "content";

    writeFile(fileSystem, TEST_FILE_PATH, fileContent);

    assertThat(readWholeFile(fileSystem, TEST_FILE_PATH)).isEqualTo(fileContent);
  }

  @Test
  public void appendWorks() throws IOException {
    String basicContent = "content";
    String appendedContent = "appended";

    writeFile(fileSystem, TEST_FILE_PATH, basicContent);
    appendFile(fileSystem, TEST_FILE_PATH, appendedContent);

    assertThat(readWholeFile(fileSystem, TEST_FILE_PATH)).isEqualTo(basicContent + appendedContent);
  }

  @After
  public void tearDown() throws IOException {
    if(fileSystem != null) {
      fileSystem.delete(TEST_FILE_PATH, false);
    }
  }
}