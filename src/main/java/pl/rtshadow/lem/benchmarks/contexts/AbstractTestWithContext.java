package pl.rtshadow.lem.benchmarks.contexts;

import com.google.inject.Inject;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import pl.rtshadow.lem.benchmarks.guice.GuiceJUnitRunner;

import java.io.IOException;

@RunWith(GuiceJUnitRunner.class)
public class AbstractTestWithContext {
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
