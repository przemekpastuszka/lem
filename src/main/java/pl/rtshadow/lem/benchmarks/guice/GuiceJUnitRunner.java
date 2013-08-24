package pl.rtshadow.lem.benchmarks.guice;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;

public class GuiceJUnitRunner extends BlockJUnit4ClassRunner {
  private Class<?> klass;

  public GuiceJUnitRunner(Class<?> klass) throws InitializationError {
    super(klass);
    this.klass = klass;
  }

  @Override
  protected Object createTest() throws Exception {
    return getInstance(klass);
  }
}
