package pl.rtshadow.lem.benchmarks.benchmarks;


import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Lists.transform;
import static java.lang.Long.parseLong;

public class ResultsMerger {

  public static void main(String[] args) throws Exception {
    new ResultsMerger().mergeFromFiles(newArrayList(args));
  }

  public void mergeFromFiles(List<String> files) throws IOException {
    List<List<String>> benchmarkResults = transform(files, new Function<String, List<String>>() {
      @Override
      public List<String> apply(String input) {
        return ResultReaderWriter.read(input);
      }
    });

    List<String> allResults = flatten(benchmarkResults);
    Collections.sort(allResults);
    final long startTime = parseLong(allResults.get(0));

    allResults = transform(allResults, new Function<String, String>() {
      @Override
      public String apply(java.lang.String input) {
        return Long.toString(TimeUnit.NANOSECONDS.toMillis(parseLong(input) - startTime));
      }
    });

    String lastValue = "-1";
    int i = 0;
    List<String> mergedResults = newArrayList();
    for (String result : allResults) {
      if (!result.equals(lastValue)) {
        mergedResults.add(result + "," + i);
      }
      lastValue = result;
      ++i;
    }

    int sizeOfMergedResults = mergedResults.size();
    int skipSize = (int) (sizeOfMergedResults / 100.0);
    List<String> chosenResults = newArrayList();
    for(i = 0; i < sizeOfMergedResults; i += skipSize) {
       chosenResults.add(mergedResults.get(i));
    }

    ResultReaderWriter.write(chosenResults, "/tmp/merge_result.csv");
  }

  private static <T> List<T> flatten(List<List<T>> lists) {
    List<T> result = newLinkedList();
    for (List<T> list : lists) {
      result.addAll(list);
    }
    return result;
  }
}
