package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;

import com.willmolloy.backup.util.DirectoryWalker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Backup local storage to Network Attached Storage.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public class LocalToNas implements FileBackup<Path, Path> {

  private static final Logger log = LogManager.getLogger();

  private final DirectoryWalker directoryWalker = new DirectoryWalker();

  @Override
  public void backup(Path source, Path destination) {
    processSource(source, destination);
    processDestination(source, destination);
  }

  private void processSource(Path source, Path destination) {
    log.info("Walking source: {}", source);
    directoryWalker
        .leavesExcludingSelf(source)
        .forEach(
            sourcePath -> {
              Path relativeFromSource = source.relativize(sourcePath);
              Path destinationPath = destination.resolve(relativeFromSource);

              try {
                if (Files.exists(sourcePath)) {
                  if (!Files.exists(destinationPath)) {
                    log.info("Creating backup: {} -> {}", sourcePath, destinationPath);
                    copy(sourcePath, destinationPath);
                  } else if (!Files.isDirectory(sourcePath)
                      && differentContents(sourcePath, destinationPath)) {
                    log.info("Updating backup: {} -> {}", sourcePath, destinationPath);
                    copy(sourcePath, destinationPath);
                  }
                }
              } catch (RuntimeException e) {
                log.error("Error backing up: %s".formatted(sourcePath), e);
              }
            });
  }

  private void copy(Path source, Path destination) {
    try {
      Files.createDirectories(checkNotNull(destination.getParent()));
      Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      log.error("Error copying: %s -> %s".formatted(source, destination), e);
    }
  }

  private boolean differentContents(Path source, Path destination) {
    try {
      return Files.mismatch(source, destination) != -1;
    } catch (IOException e) {
      log.error("Error comparing: %s to %s".formatted(source, destination), e);
      // may as well perform the backup, it doesn't make the code incorrect, only potentially slower
      return true;
    }
  }

  private void processDestination(Path source, Path destination) {
    log.info("Walking destination: {}", source);
    directoryWalker
        .allNodesExcludingSelf(destination)
        .forEach(
            destinationPath -> {
              Path relativeFromDestination = destination.relativize(destinationPath);
              Path sourcePath = source.resolve(relativeFromDestination);

              if (Files.exists(destinationPath) && !Files.exists(sourcePath)) {
                log.info("Deleting backup: {}", destinationPath);
                delete(destinationPath);
              }
            });
  }

  private void delete(Path path) {
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
