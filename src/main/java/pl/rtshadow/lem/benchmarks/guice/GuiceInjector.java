package pl.rtshadow.lem.benchmarks.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import pl.rtshadow.lem.benchmarks.guice.modules.InMemoryHdfsModule;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.Guice.createInjector;

public class GuiceInjector {
  private static Injector injector;

  public static <T> T getInstance(Class<T> clazz) {
    return getInjector().getInstance(clazz);
  }

  public static void setModule(AbstractModule abstractModule) {
    checkState(injector == null, "Guice module already set!");

    injector = createInjector(abstractModule);
  }

  private static Injector getInjector() {
    if(injector == null) {
      injector = Guice.createInjector(new InMemoryHdfsModule());
    }
    return injector;
  }
}
