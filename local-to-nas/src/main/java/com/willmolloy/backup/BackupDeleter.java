package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;

import com.willmolloy.backup.util.DirectoryWalker;
import com.willmolloy.backup.util.concurrent.ProducerConsumerOrchestrator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
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
    this.directoryWalker = checkNotNull(directoryWalker);
    this.dryRun = dryRun;
  }

  void deleteRedundantBackups(Path source, Path destination) {
    log.info("Processing destination: {}", destination);
    AtomicInteger deleteCount = new AtomicInteger(0);
    try {
      ProducerConsumerOrchestrator<Path> producerConsumer =
          new ProducerConsumerOrchestrator<>(
              // unlike creating the backups, need to process all nodes, not just leaves, because if
              // we delete a leaf, then unknown if we need to delete its parent too
              () -> directoryWalker.allNodesExcludingSelf(destination),
              destinationPath -> process(destinationPath, source, destination, deleteCount));
      producerConsumer.run();
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
        // clean directory first
        // Not using FileUtils.deleteDirectory/cleanDirectory, it isn't resilient to other consumer
        // threads deleting the children first (they fail to delete the entire dir if a child
        // doesn't exist and throws the NoSuchFileException)
        for (Path child : Files.list(path).toList()) {
          delete(child);
        }
      }
      Files.delete(path);
    } catch (NoSuchFileException ignored) {
      // ignoring the NoSuchFileException, the path was already deleted by another consumer (i.e.
      // another consumer that processed its parent)
    } catch (IOException e) {
      log.error("Error deleting: %s".formatted(path), e);
    }
  }
}
