package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Iterators.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.regex.Pattern.compile;

public class TestBlockPlacementPolicy extends BlockPlacementPolicy {
  private final static Pattern MANAGED_FILES_DIRECTORY = compile(createPathFor("(\\w+)") + "\\w+");

  private final BlockPlacementPolicy defaultPolicy = new BlockPlacementPolicyDefault();
  private Configuration configuration;
  private DistributedFileSystem fileSystem;
  private NetworkTopology networkTopology;

  @Override
  DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes, long blocksize) {
    List<DatanodeDescriptor> possibleLocations = getPossibleLocationsFor(srcPath);
    if(!possibleLocations.isEmpty()) {
      return formPipeline(possibleLocations, chosenNodes, numOfReplicas);
    }
    return defaultPolicy.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, blocksize);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes, HashMap<Node, Node> excludedNodes, long blocksize) {
    List<DatanodeDescriptor> possibleLocations = getPossibleLocationsFor(srcPath);
    if(!possibleLocations.isEmpty()) {
      return formPipeline(possibleLocations, chosenNodes, numOfReplicas);
    }
    return defaultPolicy.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, excludedNodes, blocksize);
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
    this.networkTopology = clusterMap;
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

  private DatanodeDescriptor[] formPipeline(List<DatanodeDescriptor> desiredNodes, List<DatanodeDescriptor> chosenNodes, int numOfReplicas) {
    return toArray(limit(concat(desiredNodes.iterator(), chosenNodes.iterator()), numOfReplicas), DatanodeDescriptor.class);
  }

  private List<DatanodeDescriptor> getPossibleLocationsFor(String srcPath) {
    try {
      if (isManaged(srcPath)) {
        String fileGroup = extractFileGroupName(srcPath);
        int nextBlockId = computeNumberOfNextBlock(srcPath);

        final Collection<String> possibleLocationsHosts = retrieveLocationsFor(listGroup(fileGroup), nextBlockId);

        return newArrayList(transform(possibleLocationsHosts, new Function<String, DatanodeDescriptor>() {
          @Override
          public DatanodeDescriptor apply(String location) {
            return (DatanodeDescriptor) networkTopology.getNode(location);
          }
        }));
      }
    } catch (IOException e) {
      throw new RuntimeException();
    }
    return emptyList();
  }

  private Collection<String> retrieveLocationsFor(FileStatus[] files, int blockId) throws IOException {
    Collection<String> blockLocations = newHashSet();
    for (FileStatus file : files) {
      BlockLocation[] blocks = getFileSystem().getFileBlockLocations(file, 0, 100);
      if (blocks.length > blockId) {
        blockLocations.addAll(newArrayList(blocks[blockId].getTopologyPaths()));
      }
    }
    return blockLocations;
  }

  private int computeNumberOfNextBlock(String path) {
    try {
      BlockLocation[] fileStatus = getFileSystem().getFileBlockLocations(getFileSystem().getFileStatus(new Path(path)), 0, 100);
      return fileStatus.length;
    } catch (IOException e) {
      return 0;
    }
  }

  private FileStatus[] listGroup(String fileGroupName) throws IOException {
    return getFileSystem().listStatus(new Path(createPathFor(fileGroupName)));
  }

  private String extractFileGroupName(String path) {
    Matcher matcher = MANAGED_FILES_DIRECTORY.matcher(path);
    matcher.matches();
    return matcher.group(1);
  }

  private boolean isManaged(String path) {
    return MANAGED_FILES_DIRECTORY.matcher(path).matches();
  }
}
