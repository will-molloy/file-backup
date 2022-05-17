package com.willmolloy.backup.util;

import com.google.common.collect.Streams;
import com.google.common.graph.Traverser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
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
   * Lazily walks the directory, returning all nodes, excluding itself.
   *
   * @param directory directory to walk
   * @return all nodes of the directory, excluding itself
   */
  public Stream<Path> allNodesExcludingSelf(Path directory) {
    return safeWalk(directory).filter(path -> path != directory);
  }

  /**
   * Lazily walks the directory, returning leaves (files or empty directories), excluding itself.
   *
   * @param directory directory to walk
   * @return leaves of the directory, excluding itself
   */
  public Stream<Path> leavesExcludingSelf(Path directory) {
    return allNodesExcludingSelf(directory).filter(this::isLeaf);
  }

  @SuppressWarnings("UnstableApiUsage")
  private Stream<Path> safeWalk(Path directory) {
    // Not using Files.walk or MoreFiles.fileTraverser, they both throw an IOException when the
    // subdirectory Files.list fails. Don't want one bad directory to stop the entire backup.
    Traverser<Path> fileTraverser =
        Traverser.forTree(
            node -> {
              try {
                if (Files.isDirectory(node, LinkOption.NOFOLLOW_LINKS)) {
                  return Files.list(node).toList();
                }
              } catch (IOException e) {
                log.warn("Error listing directory: %s".formatted(node), e);
              }
              return List.of();
            });

    return Streams.stream(fileTraverser.depthFirstPreOrder(directory))
        // filter out symbolic links here
        .filter(path -> !Files.isSymbolicLink(path));
  }

  private boolean isLeaf(Path node) {
    try {
      return Files.isRegularFile(node) || Files.list(node).findAny().isEmpty();
    } catch (IOException e) {
      log.warn("Error listing directory: %s".formatted(node), e);
      return false;
    }
  }
}
