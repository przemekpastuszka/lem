package pl.rtshadow.lem.benchmarks.benchmarks;


import java.io.*;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class ResultReaderWriter {
  public static void write(List<String> results, String filePath) throws IOException {
    File file = new File(filePath);
    file.getParentFile().mkdirs();

    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    try {
      for (String executionTime : results) {
        writer.write(executionTime);
        writer.newLine();
      }
    } finally {
      writer.close();
    }
  }

  public static List<String> read(String filePath) {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(filePath));
      try {
        List<String> results = newArrayList();
        String line;
        while ((line = reader.readLine()) != null) {
          results.add(line);
        }
        return results;
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}