package com.willmolloy.backup;

import java.nio.file.Path;

/**
 * Backup local storage to Network Attached Storage.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
class LocalToNas implements FileBackup<Path, Path> {

  private final BackupCreator backupCreator;
  private final BackupDeleter backupDeleter;

  LocalToNas(BackupCreator backupCreator, BackupDeleter backupDeleter) {
    this.backupCreator = backupCreator;
    this.backupDeleter = backupDeleter;
  }

  @Override
  public void backup(Path source, Path destination) {
    backupCreator.processSource(source, destination);
    backupDeleter.processDestination(source, destination);
  }
}
