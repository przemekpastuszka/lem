package pl.rtshadow.lem.benchmarks.contexts;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import com.google.inject.Inject;

import pl.rtshadow.lem.benchmarks.guice.GuiceJUnitRunner;

@RunWith(GuiceJUnitRunner.class)
public abstract class AbstractTestWithContext {
  @Inject
  private TestContext testContext;

  @Before
  public void setupContext() throws IOException {
    testContext.setup();
  }

  @After
  public void tearDownContext() {
    testContext.tearDown();
  }
}
