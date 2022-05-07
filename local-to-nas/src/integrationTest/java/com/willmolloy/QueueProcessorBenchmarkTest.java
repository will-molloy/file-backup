package com.willmolloy;

import com.github.javafaker.Faker;
import com.google.common.base.Stopwatch;
import com.willmolloy.backup.util.concurrent.ProducerConsumerOrchestrator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Comparing various implementations of queue processors.
 *
 * <p>Have discovered {@link Stream#parallel()} is not very good when the data size is unknown (as
 * it has difficulty dividing the work).
 *
 * <p>So considered producer-consumer approach. But then discovered the difficulty in picking number
 * of consumers.
 *
 * <p>Now exploring 'unlimited' consumer approach where a thread is effectively created for each
 * message that comes through. Similar idea to a web server that effectively creates a thread for
 * each request. (Say 'effectively' since a thread is not always created, it comes from a cached
 * thread pool.) This lets the OS manage the optimal number of threads (and therefore consumers) for
 * us.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
@Disabled
@SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
class QueueProcessorBenchmarkTest {

  /*
   * Statistics:
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

  private static final int NUM_ELEMENTS = 1000;

  private Stopwatch stopwatch;

  @BeforeEach
  void setUp() {
    stopwatch = Stopwatch.createStarted();
  }

  @AfterEach
  void tearDown() {
    log.info("Elapsed: {}", stopwatch.elapsed());
  }

  @Test
  void streams_sequentialStream() {
    streamToProcess().sequential().forEach(streamConsumer());
  }

  @Test
  void streams_parallelStream() {
    streamToProcess().parallel().forEach(streamConsumer());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 16, 32, 64, 128})
  void producerConsumer_fixedNumberOfConsumers(int numberOfConsumers) {
    new ProducerConsumerOrchestrator<>(() -> streamToProcess(), streamConsumer())
        .run(numberOfConsumers);
  }

  @Test
  void producerConsumer_unlimitedConsumersViaCachedThreadPool() {
    new ProducerConsumerOrchestrator<>(() -> streamToProcess(), streamConsumer()).run(0);
  }

  private Stream<Integer> streamToProcess() {
    // purposely make the stream unsized, then stream.parallel has a harder time dividing the work
    // (like in the actual app (traversing file tree))
    return IntStream.iterate(1, i -> i <= NUM_ELEMENTS, i -> i + 1)
        .mapToObj(
            i -> {
              try {
                // simulate processing
                // there is enough trials that the random number doesn't affect the result
                // however, want randomness to test working sharing/stealing
                Thread.sleep(Faker.instance().number().numberBetween(50, 100));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return i;
            });
  }

  private Consumer<Integer> streamConsumer() {
    return i -> log.info("Processed: {}", i);
  }
}
