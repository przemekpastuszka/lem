package org.apache.hadoop.hdfs.server.blockmanagement;

import static java.util.Collections.EMPTY_LIST;
import static org.fest.assertions.Assertions.assertThat;
import static org.fest.util.Collections.list;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.ColocateAppropriateBlocksOfTheSameGroupPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import pl.rtshadow.lem.benchmarks.hdfs.FileSystemService;

@RunWith(MockitoJUnitRunner.class)
public class ColocateAppropriateBlocksOfTheSameGroupPolicyTest {
  private static final int BLOCKSIZE = 512;
  private static final int NUM_OF_REPLICAS = 3;
  private static final String MANAGED_FIRST_FILE_PATH = "managed/A/first";
  private static final String MANAGED_SECOND_FILE_PATH = "managed/A/second";
  private static final String NON_MANAGED_FILE_PATH = "somePath";
  private static final String NODE_A_NAME = "nodeA";
  private static final String NODE_B_NAME = "nodeB";

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
  private ColocateAppropriateBlocksOfTheSameGroupPolicy colocateAppropriateBlocksOfTheSameGroupPolicy;

  @Before
  public void setup() throws IOException {
    when(fileSystemService.getFileSystem(configuration)).thenReturn(distributedFileSystem);
    colocateAppropriateBlocksOfTheSameGroupPolicy.initialize(configuration, null, networkTopology);

    when(networkTopology.getNode(NODE_A_NAME)).thenReturn(datanodeA);
    when(networkTopology.getNode(NODE_B_NAME)).thenReturn(datanodeB);

    prepareFile(MANAGED_FIRST_FILE_PATH, firstFileStatus, EMPTY_LIST);
    prepareFile(MANAGED_SECOND_FILE_PATH, secondFileStatus, EMPTY_LIST);
    when(distributedFileSystem.listStatus(new Path("managed/A"))).thenReturn(new FileStatus[] {firstFileStatus, secondFileStatus});
  }

  @Test
  public void delegatesNonManagedPathToDefaultPlacementPolicy() throws IOException {
    setupFallbackFor(NON_MANAGED_FILE_PATH);

    DatanodeDescriptor[] result = colocateAppropriateBlocksOfTheSameGroupPolicy.chooseTarget(NON_MANAGED_FILE_PATH, NUM_OF_REPLICAS, writer, EMPTY_LIST, BLOCKSIZE);

    assertThat(result).hasSize(1).containsOnly(fallbackDatanode);
  }

  @Test
  public void fallsBackToDefaultPlacementPolicyIfNoOtherBlockWasCreated() throws IOException {
    DatanodeDescriptor[] result = colocateAppropriateBlocksOfTheSameGroupPolicy.chooseTarget(MANAGED_FIRST_FILE_PATH, NUM_OF_REPLICAS, writer, EMPTY_LIST, BLOCKSIZE);

    assertThat(result).hasSize(1).containsOnly(fallbackDatanode);
  }

  @Test
  public void choosesSameNodesForBlockOfOtherFile() throws IOException {
    prepareFile(MANAGED_FIRST_FILE_PATH, firstFileStatus, list(list(NODE_A_NAME, NODE_B_NAME)));

    DatanodeDescriptor[] result = colocateAppropriateBlocksOfTheSameGroupPolicy.chooseTarget(MANAGED_SECOND_FILE_PATH, NUM_OF_REPLICAS, writer, EMPTY_LIST, BLOCKSIZE);

    assertThat(result).hasSize(2).containsOnly(datanodeA, datanodeB);
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
