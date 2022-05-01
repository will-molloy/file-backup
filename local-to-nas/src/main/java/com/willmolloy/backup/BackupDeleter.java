package com.willmolloy.backup;

import com.willmolloy.backup.util.DirectoryWalker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Deletes backups from Source -> Destination.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
class BackupDeleter {

  private static final Logger log = LogManager.getLogger();

  private final DirectoryWalker directoryWalker;
  private final boolean dryRun;

  BackupDeleter(DirectoryWalker directoryWalker, boolean dryRun) {
    this.directoryWalker = directoryWalker;
    this.dryRun = dryRun;
  }

  void processDestination(Path source, Path destination) {
    log.info("Processing destination: {}", source);
    AtomicInteger deleteCount = new AtomicInteger(0);
    try {
      directoryWalker
          .allNodesExcludingSelf(destination)
          // TODO is this safe???
          .parallel()
          .forEach(
              destinationPath -> {
                Path relativeFromDestination = destination.relativize(destinationPath);
                Path sourcePath = source.resolve(relativeFromDestination);

                if (Files.exists(destinationPath) && !Files.exists(sourcePath)) {
                  log.info("Deleting backup: {}", destinationPath);
                  delete(destinationPath);
                  deleteCount.incrementAndGet();
                }
              });
    } finally {
      log.info("Deleted {} backup(s)", deleteCount.get());
    }
  }

  private void delete(Path path) {
    if (dryRun) {
      return;
    }
    try {
      if (Files.isDirectory(path)) {
        FileUtils.deleteDirectory(path.toFile());
      } else {
        Files.delete(path);
      }
    } catch (IOException e) {
      log.error("Error deleting: %s".formatted(path), e);
    }
  }
}
