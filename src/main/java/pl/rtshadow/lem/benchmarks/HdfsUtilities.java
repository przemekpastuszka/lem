package pl.rtshadow.lem.benchmarks;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.EOFException;
import java.io.IOException;

public class HdfsUtilities {
  public static void writeFile(FileSystem fileSystem, Path path, String content) throws IOException {
    writeAndClose(content, fileSystem.create(path));
  }

  public static void appendFile(FileSystem fileSystem, Path path, String content) throws IOException {
    writeAndClose(content, fileSystem.append(path));
  }

  private static void writeAndClose(String content, FSDataOutputStream file) throws IOException {
    try {
      Text.writeString(file, content);
    } finally {
      file.close();
    }
  }

  public static String readWholeFile(FileSystem fileSystem, Path path) throws IOException {
    FSDataInputStream file = fileSystem.open(path);
    try {
      StringBuilder builder = new StringBuilder();
      while (true) {
        try {
          builder.append(Text.readString(file));
        } catch (EOFException ex) {
          break;
        }
      }
      return builder.toString();
    } finally {
      file.close();
    }
  }
}
