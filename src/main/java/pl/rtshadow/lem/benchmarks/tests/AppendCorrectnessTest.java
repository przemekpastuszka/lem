package pl.rtshadow.lem.benchmarks.tests;

import static java.util.Collections.nCopies;
import static org.apache.hadoop.util.StringUtils.join;
import static org.fest.assertions.Assertions.assertThat;
import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;
import static pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities.readWholeFile;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import pl.rtshadow.lem.benchmarks.contexts.AbstractTestWithContext;
import pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities;

public class AppendCorrectnessTest extends AbstractTestWithContext {
  public static final String BASIC_CONTENT = "content";
  public static final String APPENDED_DATA = "appendedData";
  public static final int NUMBER_OF_APPENDS = 100;

  private Path testFile;

  private FileSystem fileSystem;
  private FSDataOutputStream appender;

  @Before
  public void setup() throws IOException {
    fileSystem = getInstance(FileSystem.class);

    testFile = new Path("test_file" + UUID.randomUUID());
    HdfsUtilities.writeFile(fileSystem, testFile, BASIC_CONTENT);

    appender = fileSystem.append(testFile);
  }

  @Test
  public void appendedDataIsNotVisibleIfNoSyncOrCloseCalled() throws Exception {
    try {
      for (int i = 0; i < NUMBER_OF_APPENDS; ++i) {
        Text.writeString(appender, APPENDED_DATA);
        assertThat(readWholeFile(fileSystem, testFile)).isEqualTo(BASIC_CONTENT);
      }
    } finally {
      appender.close();
    }
  }

  @Test
  public void appendedDataIsVisibleAfterSync() throws IOException {
    try {
      StringBuilder expected = new StringBuilder(BASIC_CONTENT);
      for (int i = 0; i < NUMBER_OF_APPENDS; ++i) {
        Text.writeString(appender, APPENDED_DATA);
        appender.hsync();

        expected.append(APPENDED_DATA);
        assertThat(readWholeFile(fileSystem, testFile)).isEqualTo(expected.toString());
      }
    } finally {
      appender.close();
    }
  }

  @Test
  public void appendedDataIsVisibleAfterFlush() throws IOException {
    try {
      StringBuilder expected = new StringBuilder(BASIC_CONTENT);
      for (int i = 0; i < NUMBER_OF_APPENDS; ++i) {
        Text.writeString(appender, APPENDED_DATA);
        appender.hflush();

        expected.append(APPENDED_DATA);
        assertThat(readWholeFile(fileSystem, testFile)).isEqualTo(expected.toString());
      }
    } finally {
      appender.close();
    }
  }

  @Test
  public void appendedDataIsVisibleAfterClose() throws IOException {
    for (int i = 0; i < NUMBER_OF_APPENDS; ++i) {
      Text.writeString(appender, APPENDED_DATA);
    }
    appender.close();

    String expected = BASIC_CONTENT + join("", nCopies(NUMBER_OF_APPENDS, APPENDED_DATA));
    assertThat(readWholeFile(fileSystem, testFile)).isEqualTo(expected);
  }

  @After
  public void tearDown() throws IOException {
    if (testFile != null) {
      fileSystem.delete(testFile, false);
    }
  }
}
