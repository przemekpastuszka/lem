package pl.rtshadow.lem.benchmarks.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.TestBlockPlacementPolicy;

public class TestHdfsCluster {
  private static final int NUM_DATA_NODES = 5;
  private MiniDFSCluster miniDFSCluster;

  public void start() throws IOException {
    Configuration configuration = new Configuration();
    configuration.setBoolean("dfs.support.broken.append", true);
    configuration.setInt("dfs.datanode.data.dir.perm", 775);
    configuration.setClass("dfs.block.replicator.classname", TestBlockPlacementPolicy.class, BlockPlacementPolicy.class);
    // set block size to small value to test more interesting cases
    configuration.setInt("dfs.block.size", 512);

    miniDFSCluster = new MiniDFSCluster(configuration, NUM_DATA_NODES, true, null);
    miniDFSCluster.waitActive();
  }

  public FileSystem getFileSystem() throws IOException {
    return miniDFSCluster.getFileSystem();
  }

  public void stop() {
    miniDFSCluster.shutdown();
  }
}
