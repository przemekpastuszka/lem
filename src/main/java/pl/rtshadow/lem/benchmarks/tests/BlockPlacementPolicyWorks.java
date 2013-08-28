package pl.rtshadow.lem.benchmarks.tests;


import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import pl.rtshadow.lem.benchmarks.contexts.AbstractTestWithContext;

import java.io.IOException;

import static com.google.common.collect.Sets.newHashSet;
import static org.fest.assertions.Assertions.assertThat;
import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;

public class BlockPlacementPolicyWorks extends AbstractTestWithContext {
  public static final int NUMBER_OF_WRITES = 100;
  public static final Path X_FILE_PATH = new Path("/managed/A/x");
  public static final Path Y_FILE_PATH = new Path("/managed/A/y");

  private FileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    fileSystem = getInstance(FileSystem.class);

    fileSystem.mkdirs(new Path("/managed"));
    fileSystem.mkdirs(new Path("/managed/A"));
    fileSystem.mkdirs(new Path("/managed/B"));
  }

  @Test
  public void placesBlocksTogether() throws IOException {
    FSDataOutputStream fileX = fileSystem.create(X_FILE_PATH);
    FSDataOutputStream fileY = fileSystem.create(Y_FILE_PATH);

    for (int i = 0; i < NUMBER_OF_WRITES; ++i) {
      Text.writeString(fileX, "newDataX");
      Text.writeString(fileY, "newDataY");
    }

    fileX.close();
    fileY.close();

    BlockLocation[] blocksX = getFileBlocksFor(X_FILE_PATH);
    BlockLocation[] blocksY = getFileBlocksFor(Y_FILE_PATH);

    for (int i = 0; i < blocksX.length && i < blocksY.length; ++i) {
      assertThat(newHashSet(blocksX[i].getNames())).isEqualTo(newHashSet(blocksY[i].getNames()));
    }
  }

  private BlockLocation[] getFileBlocksFor(Path path) throws IOException {
    return fileSystem.getFileBlockLocations(fileSystem.getFileStatus(path), 0, 1000);
  }
}
