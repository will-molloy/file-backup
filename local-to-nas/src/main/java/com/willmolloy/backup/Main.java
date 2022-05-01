package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Stopwatch;
import com.willmolloy.backup.util.DirectoryWalker;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Entry point.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
final class Main {
  private Main() {}

  private static final Logger log = LogManager.getLogger();

  public static void main(String... args) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      checkArgument(args.length == 3, "Expected 3 args");
      Path source = Path.of(args[0]);
      Path destination = Path.of(args[1]);
      boolean dryRun = Boolean.parseBoolean(args[2]);
      checkArgument(
          Files.exists(source) && Files.isDirectory(source),
          "Expected source (%s) to be a directory");
      checkArgument(
          Files.exists(destination) && Files.isDirectory(destination),
          "Expected destination (%s) to be a directory");

      DirectoryWalker directoryWalker = new DirectoryWalker();
      BackupCreator backupCreator = new BackupCreator(directoryWalker, dryRun);
      BackupDeleter backupDeleter = new BackupDeleter(directoryWalker, dryRun);
      LocalToNas localToNas = new LocalToNas(backupCreator, backupDeleter);

      log.info(
          "Running backup - source={}, destination={}, dryRun={}", source, destination, dryRun);
      localToNas.backup(source, destination);
      log.info("Backup complete");
    } catch (Throwable t) {
      log.fatal("Fatal error", t);
    } finally {
      log.info("Elapsed: {}", stopwatch.elapsed());
    }
  }
}
