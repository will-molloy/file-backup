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

  /*
   * Statistics (1M files copied, Ryzen 9 5950x):
   * - Sequential:
   * - Parallel streams:
   * - Producer-consumer, 1 Producer/1 Consumer:
   * - Producer-consumer, 1 Producer/2 Consumers:
   * - Producer-consumer, 1 Producer/4 Consumers:
   * - Producer-consumer, 1 Producer/8 Consumers:
   * - Producer-consumer, 1 Producer/16 Consumers:
   * - Producer-consumer, 1 Producer/32 Consumers:
   * - Producer-consumer, 1 Producer/64 Consumers:
   * - Producer-consumer, 1 Producer/128 Consumers:
   * - Producer-consumer, 1 Producer/1 Unbounded Consumer:
   */

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
