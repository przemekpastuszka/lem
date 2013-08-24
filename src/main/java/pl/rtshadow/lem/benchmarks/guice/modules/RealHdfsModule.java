package pl.rtshadow.lem.benchmarks.guice.modules;

import com.google.inject.AbstractModule;
import org.apache.hadoop.fs.FileSystem;
import pl.rtshadow.lem.benchmarks.contexts.TestContext;

import java.io.IOException;

public class RealHdfsModule extends AbstractModule implements TestContext {
  private final FileSystem fileSystem;

  public RealHdfsModule(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @Override
  protected void configure() {
    bind(TestContext.class).to(RealHdfsModule.class);
    bind(FileSystem.class).toInstance(fileSystem);
  }

  @Override
  public void setup() throws IOException {
    // do nothing
  }

  @Override
  public void tearDown() {
    // do nothing
  }
}
