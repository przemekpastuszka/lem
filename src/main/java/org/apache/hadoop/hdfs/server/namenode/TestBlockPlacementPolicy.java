package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.regex.Pattern.compile;

public class TestBlockPlacementPolicy extends BlockPlacementPolicy {
  private final static Pattern MANAGED_FILES_DIRECTORY = compile(createPathFor("(\\w+)"));

  private final BlockPlacementPolicy defaultPolicy = new BlockPlacementPolicyDefault();
  private Configuration configuration;
  private DistributedFileSystem fileSystem;

  @Override
  DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes, long blocksize) {
    List<DatanodeDescriptor> possibleLocations = getPossibleLocationsFor(srcPath, chosenNodes);
    return defaultPolicy.chooseTarget(srcPath, numOfReplicas, writer, possibleLocations, blocksize);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes, HashMap<Node, Node> excludedNodes, long blocksize) {
    List<DatanodeDescriptor> possibleLocations = getPossibleLocationsFor(srcPath, chosenNodes);
    return defaultPolicy.chooseTarget(srcPath, numOfReplicas, writer, possibleLocations, excludedNodes, blocksize);
  }

  @Override
  public int verifyBlockPlacement(String srcPath, LocatedBlock lBlk, int minRacks) {
    return defaultPolicy.verifyBlockPlacement(srcPath, lBlk, minRacks);
  }

  @Override
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo srcInode, Block block, short replicationFactor, Collection<DatanodeDescriptor> existingReplicas, Collection<DatanodeDescriptor> moreExistingReplicas) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap) {
    this.configuration = conf;
    defaultPolicy.initialize(conf, stats, clusterMap);
  }

  private DistributedFileSystem getFileSystem() {
    if (fileSystem == null) {
      try {
        fileSystem = (DistributedFileSystem) DistributedFileSystem.get(configuration);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return fileSystem;
  }

  private static String createPathFor(String fileGroupName) {
    return format("/managed/%s/", fileGroupName);
  }

  private List<DatanodeDescriptor> getPossibleLocationsFor(String srcPath, List<DatanodeDescriptor> chosenNodes) {
    try {
      if (isManaged(srcPath)) {
        String fileGroup = extractFileGroupName(srcPath);
        int nextBlockId = computeNumberOfNextBlock(srcPath);

        final Collection<String> possibleLocationsHosts = retrieveLocationsFor(listGroup(fileGroup), nextBlockId);

        ArrayList<DatanodeDescriptor> possibleLocations = newArrayList(
            filter(chosenNodes, new Predicate<DatanodeDescriptor>() {
              @Override
              public boolean apply(DatanodeDescriptor input) {
                return possibleLocationsHosts.contains(input.getHostName());
              }
            }));

        if (possibleLocations.isEmpty()) {
          return chosenNodes;
        } else {
          return possibleLocations;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException();
    }
    return chosenNodes;
  }

  private Collection<String> retrieveLocationsFor(FileStatus[] files, int blockId) throws IOException {
    Collection<String> blockLocations = newHashSet();
    for (FileStatus file : files) {
      BlockLocation[] blocks = getFileSystem().getFileBlockLocations(file, 0, 100);
      if (blocks.length > blockId) {
        blockLocations.addAll(newArrayList(blocks[blockId].getHosts()));
      }
    }
    return blockLocations;
  }

  private int computeNumberOfNextBlock(String path) {
    try {
      BlockLocation[] fileStatus = getFileSystem().getFileBlockLocations(getFileSystem().getFileStatus(new Path(path)), 0, 100);
      return fileStatus.length + 1;
    } catch (IOException e) {
      return 0;
    }
  }

  private FileStatus[] listGroup(String fileGroupName) throws IOException {
    return getFileSystem().listStatus(new Path(createPathFor(fileGroupName)));
  }

  private String extractFileGroupName(String path) {
    return MANAGED_FILES_DIRECTORY.matcher(path).group(1);
  }

  private boolean isManaged(String path) {
    return MANAGED_FILES_DIRECTORY.matcher(path).matches();
  }
}
