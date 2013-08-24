package pl.rtshadow.lem.benchmarks.guice.modules;


import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.hadoop.fs.FileSystem;
import pl.rtshadow.lem.benchmarks.contexts.TestContext;
import pl.rtshadow.lem.benchmarks.hdfs.TestHdfsCluster;

import java.io.IOException;

import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;

public class InMemoryHdfsModule extends AbstractModule implements TestContext {
  private TestHdfsCluster testHdfsCluster;

  @Override
  public void setup() throws IOException {
    Preconditions.checkState(testHdfsCluster == null, "Cluster already started!");

    testHdfsCluster = getInstance(TestHdfsCluster.class);
    testHdfsCluster.start();
  }

  @Override
  public void tearDown() {
    if (testHdfsCluster != null) {
      testHdfsCluster.stop();
    }
    testHdfsCluster = null;
  }

  @Override
  protected void configure() {
    bind(TestContext.class).to(InMemoryHdfsModule.class);
    bind(TestHdfsCluster.class).toInstance(new TestHdfsCluster());
  }

  @Provides
  public FileSystem provideFileSystem(TestHdfsCluster cluster) throws IOException {
    return cluster.getFileSystem();
  }
}
