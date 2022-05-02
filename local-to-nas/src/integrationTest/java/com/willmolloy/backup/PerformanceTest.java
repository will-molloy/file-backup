package com.willmolloy.backup;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Manual performance testing.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
class PerformanceTest extends BaseIntegrationTest {

  private static final Logger log = LogManager.getLogger();

  private static final int NUM_FILES = 1_000_000;

  @Disabled
  @Test
  void run() throws IOException {
    if (Files.list(source).count() != NUM_FILES) {
      log.info("Recreating data");
      IntStream.range(0, NUM_FILES)
          .forEach(i -> createRandomFileOrDirectoryUnder(source.resolve(String.valueOf(i))));
    }

    runApp(false);
  }

  @Override
  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(destination.toFile());
    // comment out to skip setup on subsequent runs
    //    FileUtils.deleteDirectory(source.toFile());
  }
}
