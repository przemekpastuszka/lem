package pl.rtshadow.lem.benchmarks.tests;


import static com.google.common.collect.Sets.newHashSet;
import static org.fest.assertions.Assertions.assertThat;
import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import pl.rtshadow.lem.benchmarks.contexts.AbstractTestWithContext;
import pl.rtshadow.lem.benchmarks.hdfs.HdfsUtilities;

public class BlockPlacementPolicyWorksTest extends AbstractTestWithContext {
  public static final int NUMBER_OF_WRITES = 100;
  public static final String NEW_DATA = "newData";
  private Path X_FILE_PATH;
  private Path Y_FILE_PATH;

  private FileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    fileSystem = getInstance(FileSystem.class);

    X_FILE_PATH = new Path("/managed/A/x" + UUID.randomUUID());
    Y_FILE_PATH = new Path("/managed/A/y" + UUID.randomUUID());

    fileSystem.mkdirs(new Path("/managed"));
    fileSystem.mkdirs(new Path("/managed/A"));
    fileSystem.mkdirs(new Path("/managed/B"));
  }

  @Test
  public void placesBlocksTogetherNoClose() throws IOException {
    FSDataOutputStream fileX = fileSystem.create(X_FILE_PATH);
    FSDataOutputStream fileY = fileSystem.create(Y_FILE_PATH);

    for (int i = 0; i < NUMBER_OF_WRITES; ++i) {
      Text.writeString(fileX, NEW_DATA);
      Text.writeString(fileY, NEW_DATA);
    }

    fileX.close();
    fileY.close();

    checkBlocksAreColocated();
  }

  @Test
  public void placesBlocksTogetherWithCloseAfterEachWrite() throws IOException {
    HdfsUtilities.writeFile(fileSystem, X_FILE_PATH, "");
    HdfsUtilities.writeFile(fileSystem, Y_FILE_PATH, "");

    for (int i = 0; i < NUMBER_OF_WRITES; ++i) {
      HdfsUtilities.appendFile(fileSystem, X_FILE_PATH, NEW_DATA);
      HdfsUtilities.appendFile(fileSystem, Y_FILE_PATH, NEW_DATA);
    }

    checkBlocksAreColocated();
  }

  private void checkBlocksAreColocated() throws IOException {
    BlockLocation[] blocksX = getFileBlocksFor(X_FILE_PATH);
    BlockLocation[] blocksY = getFileBlocksFor(Y_FILE_PATH);

    for (int i = 0; i < blocksX.length && i < blocksY.length; ++i) {
      assertThat(newHashSet(blocksX[i].getNames())).as("Locations inequality at " + i).isEqualTo(newHashSet(blocksY[i].getNames()));
    }
  }

  private BlockLocation[] getFileBlocksFor(Path path) throws IOException {
    return fileSystem.getFileBlockLocations(fileSystem.getFileStatus(path), 0, 1000);
  }
}
