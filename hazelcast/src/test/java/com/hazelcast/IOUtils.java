package com.hazelcast;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.function.Function;

public class IOUtils {
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static File createConfigFile(String filename, String suffix) throws IOException {
    File file = File.createTempFile(filename, suffix);
    file.setWritable(true);
    file.deleteOnExit();
    return file;
  }

  public static void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
    os.write(string.getBytes());
    os.flush();
    os.close();
  }

  public static String createFileWithContent(String filename, String suffix, String content) throws IOException {
    File file = createConfigFile(filename, suffix);
    FileOutputStream os = new FileOutputStream(file);
    writeStringToStreamAndClose(os, content);
    return file.getAbsolutePath();
  }

  public static String createFilesWithCycleImports(Function<String, String> fileContentWithImportResource, String... paths) throws Exception {
    for (int i = 1; i < paths.length; i++) {
      createFileWithDependencyImport(paths[i - 1], paths[i], fileContentWithImportResource);
    }
    return createFileWithDependencyImport(paths[0], paths[1], fileContentWithImportResource);
  }

  private static String createFileWithDependencyImport(
      String dependent,
      String pathToDependency,
      Function<String, String> fileContentWithImportResource) throws Exception {
    final String xmlContent = fileContentWithImportResource.apply(pathToDependency);
    IOUtils.writeStringToStreamAndClose(new FileOutputStream(dependent), xmlContent);
    return xmlContent;
  }
}
