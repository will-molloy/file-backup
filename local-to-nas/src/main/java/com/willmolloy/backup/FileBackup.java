package com.willmolloy.backup;

import java.nio.file.Path;

/**
 * File backup contract.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public interface FileBackup {

  void backup(Path source, Path destination);
}
