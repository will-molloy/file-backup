package com.willmolloy;

import com.github.javafaker.Faker;
import com.willmolloy.backup.util.concurrent.ProducerConsumerOrchestrator;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
 * <p>Concern with this approach is it creates too many threads for the OS/hardware to handle. I'd
 * expect {@link Executors#newCachedThreadPool()} to handle that.
 *
 * <p>Also, why doesn't the fixed number of consumers work? I.e. why does setting more threads than
 * the CPU has cores work? It's because the OS is constantly context switching and working on
 * multiple Java threads at once.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
@Disabled
class QueueProcessorBenchmarkTest {

  /*
   * Statistics:
   * - Sequential:                                       78s
   * - Parallel streams:                                 77s (??? implemented wrong?)
   * - Producer-consumer, 1 Producer/1 Consumer:         76s
   * - Producer-consumer, 1 Producer/2 Consumers:        38.145s
   * - Producer-consumer, 1 Producer/4 Consumers:        19.147s
   * - Producer-consumer, 1 Producer/8 Consumers:        9.574s
   * - Producer-consumer, 1 Producer/16 Consumers:       4.824s
   * - Producer-consumer, 1 Producer/32 Consumers:       2.437s
   * - Producer-consumer, 1 Producer/64 Consumers:       1.233s
   * - Producer-consumer, 1 Producer/128 Consumers:      0.642s
   * - Producer-consumer, 1 Producer/Unbounded Consumer: 0.151s
   */

  private static final Logger log = LogManager.getLogger();

  @Test
  void streams_sequentialStream() {
    data().sequential().forEach(processor());
  }

  @Test
  void streams_parallelStream() {
    data().parallel().forEach(processor());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 16, 32, 64, 128})
  void producerConsumer_fixedNumberOfConsumers(int numberOfConsumers) {
    new ProducerConsumerOrchestrator<>(() -> data(), processor()).run(numberOfConsumers);
  }

  @Test
  void producerConsumer_unlimitedConsumersViaCachedThreadPool() {
    new ProducerConsumerOrchestrator<>(() -> data(), processor()).run(0);
  }

  private Stream<Integer> data() {
    // purposely make the stream unsized, then stream.parallel has a harder time dividing the work
    // (like in the actual app (traversing file tree))
    return IntStream.iterate(1, i -> i <= 1_000_000, i -> i + 1).boxed();
  }

  private Consumer<Integer> processor() {
    return i -> {
      try {
        // simulate processing
        // there is enough trials that the random number doesn't affect the result
        // (75s total processing on avg)
        // however, want randomness to test work sharing/stealing
        // NOTE: Thread.sleep is async!
        Thread.sleep(Faker.instance().number().numberBetween(50, 100));
      } catch (InterruptedException e) {
        log.error("Thread interrupted", e);
        Thread.currentThread().interrupt();
      }
      log.info("Processed: {}", i);
    };
  }
}
