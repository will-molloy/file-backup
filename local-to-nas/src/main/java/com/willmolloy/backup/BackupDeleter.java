package com.willmolloy.backup;

import com.willmolloy.backup.util.DirectoryWalker;
import com.willmolloy.infrastructure.ProducerConsumer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Deletes redundant backups (files in Destination that don't exist in Source).
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

  void deleteRedundantBackups(Path source, Path destination) {
    log.info("Processing destination: {}", source);
    AtomicInteger deleteCount = new AtomicInteger(0);
    try {
      ProducerConsumer<Path> producerConsumer =
          new ProducerConsumer<>(
              // unlike creating the backups, need to process all nodes, not just leaves
              // because if we delete a leaf, then may need to delete its parent too
              () -> directoryWalker.allNodesExcludingSelf(destination),
              destinationPath -> process(destinationPath, source, destination, deleteCount));
      producerConsumer.run();
    } catch (InterruptedException e) {
      log.error("Producer/Consumer interrupted", e);
    } finally {
      log.info("Deleted {} backup(s)", deleteCount.get());
    }
  }

  private void process(
      Path destinationPath, Path source, Path destination, AtomicInteger deleteCount) {
    Path relativeFromDestination = destination.relativize(destinationPath);
    Path sourcePath = source.resolve(relativeFromDestination);

    if (Files.exists(destinationPath) && !Files.exists(sourcePath)) {
      log.info("Deleting backup: {}", destinationPath);
      delete(destinationPath);
      deleteCount.getAndIncrement();
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
