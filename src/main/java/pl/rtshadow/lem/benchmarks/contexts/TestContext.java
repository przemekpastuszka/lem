package pl.rtshadow.lem.benchmarks.contexts;

import java.io.IOException;

public interface TestContext {
  void setup() throws IOException;

  void tearDown();
}
