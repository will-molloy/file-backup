package com.willmolloy.backup.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Directory Walker.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public class DirectoryWalker {

  private static final Logger log = LogManager.getLogger();

  /**
   * Lazily walks the directory, returning leaves (files or empty directories), excluding itself.
   *
   * @param directory directory to walk
   * @return leaves of the directory, excluding itself
   */
  public Stream<Path> leavesExcludingSelf(Path directory) {
    return allNodesExcludingSelf(directory)
        .filter(
            path -> {
              try {
                return Files.isRegularFile(path) || Files.list(path).findAny().isEmpty();
              } catch (IOException e) {
                log.error("Error listing directory: %s".formatted(path), e);
                throw new UncheckedIOException(e);
              }
            });
  }

  /**
   * Lazily walks the directory, returning all nodes, excluding itself.
   *
   * @param directory directory to walk
   * @return all nodes of the directory, excluding itself
   */
  public Stream<Path> allNodesExcludingSelf(Path directory) {
    try {
      return Files.walk(directory).filter(path -> path != directory);
    } catch (IOException e) {
      log.error("Error walking directory: %s".formatted(directory), e);
      throw new UncheckedIOException(e);
    }
  }
}
