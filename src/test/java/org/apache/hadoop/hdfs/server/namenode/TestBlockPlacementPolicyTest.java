package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.rtshadow.lem.benchmarks.hdfs.FileSystemService;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestBlockPlacementPolicyTest {
  private static final int BLOCKSIZE = 512;
  private static final int NUM_OF_REPLICAS = 3;
  private static final String MANAGED_FIRST_FILE_PATH = "managed/A/first";
  private static final String MANAGED_SECOND_FILE_PATH = "managed/A/second";
  private static final String NON_MANAGED_FILE_PATH = "somePath";

  @Mock
  private BlockPlacementPolicy defaultBlockPlacementPolicy;
  @Mock
  private Configuration configuration;
  @Mock
  private FileSystemService fileSystemService;
  @Mock
  private DistributedFileSystem distributedFileSystem;
  @Mock
  private NetworkTopology networkTopology;
  @Mock
  private FileStatus firstFileStatus, secondFileStatus;

  @Mock
  private DatanodeDescriptor writer, datanodeA, datanodeB, fallbackDatanode;

  @InjectMocks
  private TestBlockPlacementPolicy testBlockPlacementPolicy;

  @Before
  public void setup() throws IOException {
    when(fileSystemService.getFileSystem(configuration)).thenReturn(distributedFileSystem);
    testBlockPlacementPolicy.initialize(configuration, null, networkTopology);

    prepareFile(MANAGED_FIRST_FILE_PATH, firstFileStatus, EMPTY_LIST);
    prepareFile(MANAGED_SECOND_FILE_PATH, secondFileStatus, EMPTY_LIST);
    when(distributedFileSystem.listStatus(new Path("managed/A"))).thenReturn(new FileStatus[] {firstFileStatus, secondFileStatus});
  }

  @Test
  public void delegatesNonManagedPathToDefaultPlacementPolicy() throws IOException {
    setupFallbackFor(NON_MANAGED_FILE_PATH);

    DatanodeDescriptor[] result = testBlockPlacementPolicy.chooseTarget(NON_MANAGED_FILE_PATH, NUM_OF_REPLICAS, writer, EMPTY_LIST, BLOCKSIZE);

    assertThat(result).hasSize(1).containsOnly(fallbackDatanode);
  }

  @Test
  public void fallsBackToDefaultPlacementPolicyIfNoOtherBlockWasCreated() throws IOException {
    DatanodeDescriptor[] result = testBlockPlacementPolicy.chooseTarget(MANAGED_FIRST_FILE_PATH, NUM_OF_REPLICAS, writer, EMPTY_LIST, BLOCKSIZE);

    assertThat(result).hasSize(1).containsOnly(fallbackDatanode);
  }

  private void setupFallbackFor(String path) {
    when(defaultBlockPlacementPolicy.chooseTarget(path, NUM_OF_REPLICAS, writer, EMPTY_LIST, BLOCKSIZE)).
        thenReturn(new DatanodeDescriptor[]{fallbackDatanode});
  }

  private void prepareFile(String fileName, FileStatus fileStatus, List<List<String>> blockLocations) throws IOException {
    when(distributedFileSystem.getFileStatus(new Path(fileName))).thenReturn(fileStatus);
    setupFallbackFor(fileName);

    BlockLocation[] blockLocationsObjects = new BlockLocation[blockLocations.size()];
    for(int i = 0; i < blockLocations.size(); ++i) {
      blockLocationsObjects[i] = blockLocation(blockLocations.get(i));
    }

    when(distributedFileSystem.getFileBlockLocations(fileStatus, 0, Long.MAX_VALUE)).thenReturn(blockLocationsObjects);
  }

  private BlockLocation blockLocation(List<String> locations) throws IOException {
    BlockLocation blockLocation = mock(BlockLocation.class);
    when(blockLocation.getTopologyPaths()).thenReturn(locations.toArray(new String[] {}));
    return blockLocation;
  }
}
