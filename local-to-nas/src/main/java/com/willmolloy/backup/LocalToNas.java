package com.willmolloy.backup;

import java.nio.file.Path;

/**
 * Backup local storage to Network Attached Storage.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public class LocalToNas implements FileBackup {

  @Override
  public void backup(Path source, Path destination) {}
}
