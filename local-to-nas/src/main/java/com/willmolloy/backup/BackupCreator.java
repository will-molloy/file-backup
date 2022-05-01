package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;

import com.willmolloy.backup.util.DirectoryWalker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates/updates backups from Source -> Destination.
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

  void processSource(Path source, Path destination) {
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

                  } else if (differentContents(sourcePath, destinationPath)) {
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
