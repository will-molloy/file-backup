package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;

import com.willmolloy.backup.util.DirectoryWalker;
import com.willmolloy.infrastructure.ProducerConsumer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates backups (files in Source that don't exist in Destination).
 *
 * <p>And updates backups (files in both Source and Destination that aren't in sync).
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
class BackupCreator {

  private static final Logger log = LogManager.getLogger();

  private final DirectoryWalker directoryWalker;
  private final boolean dryRun;

  BackupCreator(DirectoryWalker directoryWalker, boolean dryRun) {
    this.directoryWalker = directoryWalker;
    this.dryRun = dryRun;
  }

  void createOrUpdateOutOfSyncBackups(Path source, Path destination) {
    log.info("Processing source: {}", source);
    AtomicInteger copyCount = new AtomicInteger(0);
    try {
      ProducerConsumer<Path> producerConsumer =
          new ProducerConsumer<>(
              // only need to process leaves, parent directories can be created when needed
              // also allows the code to run concurrently (it wouldn't be threadsafe otherwise,
              // subdirectories would depend on their parents being created first)
              () -> directoryWalker.leavesExcludingSelf(source),
              sourcePath -> process(sourcePath, source, destination, copyCount));
      producerConsumer.run();
    } catch (InterruptedException e) {
      log.error("Producer/Consumer interrupted", e);
    } finally {
      log.info("Created/updated {} backup(s)", copyCount.get());
    }
  }

  private void process(Path sourcePath, Path source, Path destination, AtomicInteger copyCount) {
    Path relativeFromSource = source.relativize(sourcePath);
    Path destinationPath = destination.resolve(relativeFromSource);

    if (Files.exists(sourcePath)) {

      if (!Files.exists(destinationPath)) {
        log.info("Creating backup: {} -> {}", sourcePath, destinationPath);
        copy(sourcePath, destinationPath);
        copyCount.getAndIncrement();

      } else if (outOfSync(sourcePath, destinationPath)) {
        log.info("Updating backup: {} -> {}", sourcePath, destinationPath);
        copy(sourcePath, destinationPath);
        copyCount.getAndIncrement();
      }
    }
  }

  private boolean outOfSync(Path source, Path destination) {
    try {
      // comparing last modified time and size attributes only
      // Files.mismatch is slow (and unnecessary?)
      return !Files.getLastModifiedTime(source, LinkOption.NOFOLLOW_LINKS)
              .equals(Files.getLastModifiedTime(destination, LinkOption.NOFOLLOW_LINKS))
          || Files.size(source) != Files.size(destination);
    } catch (IOException e) {
      log.warn("Error comparing: %s to %s".formatted(source, destination), e);
      return false;
    }
  }

  private void copy(Path source, Path destination) {
    if (dryRun) {
      return;
    }
    try {
      Files.createDirectories(checkNotNull(destination.getParent()));
      Files.copy(
          source,
          destination,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES);
    } catch (IOException e) {
      log.error("Error copying: %s -> %s".formatted(source, destination), e);
    }
  }
}
