package pl.rtshadow.lem.benchmarks.benchmarks;


import java.io.*;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class ResultReaderWriter {
  public static void write(List<Long> executionTimes, String filePath) throws IOException {
    File file = new File(filePath);
    file.getParentFile().mkdirs();

    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    try {
      for (Long executionTime : executionTimes) {
        writer.write(executionTime.toString());
        writer.newLine();
      }
    } finally {
      writer.close();
    }
  }

  public static List<Long> read(String filePath) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filePath));
    try {
      List<Long> results = newArrayList();
      String line;
      while ((line = reader.readLine()) != null) {
        results.add(Long.parseLong(line));
      }
      return results;
    } finally {
      reader.close();
    }
  }
}
