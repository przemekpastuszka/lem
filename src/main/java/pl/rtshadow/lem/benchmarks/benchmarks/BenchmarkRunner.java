package pl.rtshadow.lem.benchmarks.benchmarks;


import com.thoughtworks.xstream.XStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import pl.rtshadow.lem.benchmarks.contexts.AbstractTestWithContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.lang.System.nanoTime;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static pl.rtshadow.lem.benchmarks.benchmarks.ResultReaderWriter.write;
import static pl.rtshadow.lem.benchmarks.guice.GuiceInjector.getInstance;

public class BenchmarkRunner extends AbstractTestWithContext {
  private final static String RESULT_FILE_PREFIX = "/tmp/lemtest/";

  @Rule
  public TestName testName = new TestName();

  private FileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    fileSystem = getInstance(FileSystem.class);
  }

  protected void performTest(final BenchmarkCase benchmarkCase, final int executionCount, int threadCount) throws Exception {
    ExecutorService executorService = newFixedThreadPool(threadCount);
    Callable<List<Long>> singleBenchmark = new Callable<List<Long>>() {
      @Override
      public List<Long> call() throws Exception {
        return performTest(benchmarkCase, executionCount);
      }
    };

    List<Future<List<Long>>> futures = executorService.invokeAll(nCopies(threadCount, singleBenchmark));
    List<Long> result = newArrayListWithCapacity(executionCount * threadCount);
    for(Future<List<Long>> future : futures) {
       result.addAll(future.get());
    }

    write(result, RESULT_FILE_PREFIX + testName.getMethodName());
  }

  private List<Long> performTest(BenchmarkCase benchmarkCase, int executionCount) throws Exception {
    List<Long> timesOfCompletion = newArrayListWithCapacity(executionCount);

    for (int i = 0; i < executionCount; ++i) {
      benchmarkCase.run(fileSystem);
      timesOfCompletion.add(nanoTime());
    }

    return timesOfCompletion;
  }
}
