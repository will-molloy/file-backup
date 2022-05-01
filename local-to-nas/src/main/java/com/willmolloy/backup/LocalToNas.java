package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;

import com.willmolloy.backup.util.DirectoryWalker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Backup local storage to Network Attached Storage.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
class LocalToNas implements FileBackup<Path, Path> {

  private static final Logger log = LogManager.getLogger();

  private final DirectoryWalker directoryWalker = new DirectoryWalker();
  private final boolean dryRun;

  LocalToNas(boolean dryRun) {
    this.dryRun = dryRun;
  }

  @Override
  public void backup(Path source, Path destination) {
    processSource(source, destination);
    processDestination(source, destination);
  }

  private void processSource(Path source, Path destination) {
    log.info("Processing source: {}", source);
    AtomicInteger copyCount = new AtomicInteger(0);
    try {
      directoryWalker
          .leavesExcludingSelf(source)
          // TODO is this safe???
          .parallel()
          .forEach(
              sourcePath -> {
                Path relativeFromSource = source.relativize(sourcePath);
                Path destinationPath = destination.resolve(relativeFromSource);

                if (Files.exists(sourcePath)) {

                  if (!Files.exists(destinationPath)) {
                    log.info("Creating backup: {} -> {}", sourcePath, destinationPath);
                    copy(sourcePath, destinationPath);
                    copyCount.incrementAndGet();

                  } else if (!Files.isDirectory(sourcePath)
                      && differentContents(sourcePath, destinationPath)) {
                    log.info("Updating backup: {} -> {}", sourcePath, destinationPath);
                    copy(sourcePath, destinationPath);
                    copyCount.incrementAndGet();
                  }
                }
              });
    } finally {
      log.info("Created/updated {} backup(s)", copyCount.get());
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

  private boolean differentContents(Path source, Path destination) {
    try {
      // comparing last modified time and size attributes only
      // Files.mismatch is slow (and unnecessary?)
      return !Files.getLastModifiedTime(source, LinkOption.NOFOLLOW_LINKS)
              .equals(Files.getLastModifiedTime(destination, LinkOption.NOFOLLOW_LINKS))
          || Files.size(source) != Files.size(destination);
    } catch (IOException e) {
      log.warn("Error comparing: %s to %s".formatted(source, destination), e);
      // may as well perform the backup, it doesn't make the code incorrect, only potentially slower
      return true;
    }
  }

  private void processDestination(Path source, Path destination) {
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
