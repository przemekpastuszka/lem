package pl.rtshadow.lem.benchmarks;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import pl.rtshadow.lem.benchmarks.guice.GuiceInjector;
import pl.rtshadow.lem.benchmarks.guice.modules.RealHdfsModule;
import pl.rtshadow.lem.benchmarks.tests.AppendCorrectness;
import pl.rtshadow.lem.benchmarks.tests.SimpleHdfsWriteReads;

public class RealHdfsRunner extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RealHdfsRunner(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    FileSystem fileSystem = FileSystem.get(getConf());
    GuiceInjector.setModule(new RealHdfsModule(fileSystem));

    Result result = JUnitCore.runClasses(SimpleHdfsWriteReads.class, AppendCorrectness.class);
    new TextListener(System.out).testRunFinished(result);

    return result.getFailureCount() > 0 ? 1 : 0;
  }
}
