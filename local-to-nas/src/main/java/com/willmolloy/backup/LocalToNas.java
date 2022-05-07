package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;

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
    this.backupCreator = checkNotNull(backupCreator);
    this.backupDeleter = checkNotNull(backupDeleter);
  }

  @Override
  public void backup(Path source, Path destination) {
    backupCreator.createOrUpdateOutOfSyncBackups(source, destination);
    backupDeleter.deleteRedundantBackups(source, destination);
  }
}
