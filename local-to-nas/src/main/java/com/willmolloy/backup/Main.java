package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Entry point.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
final class Main {
  private Main() {}

  public static void main(String... args) {
    checkArgument(args.length == 2, "Expected 2 args");
    Path source = Path.of(args[0]);
    Path destination = Path.of(args[1]);
    checkArgument(Files.isDirectory(source), "Expected source (%s) to be a directory");
    checkArgument(Files.isDirectory(destination), "Expected destination (%s) to be a directory");

    LocalToNas localToNas = new LocalToNas();
    localToNas.backup(source, destination);
  }
}
