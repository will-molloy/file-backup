package com.willmolloy;

import com.github.javafaker.Faker;
import com.willmolloy.backup.util.concurrent.ProducerConsumerOrchestrator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
 * it has difficulty dividing the work). Also, not very good with I/O bound work (since it limits
 * the amount of parallelism to the CPU core count).
 *
 * <p>So considered producer-consumer approach. But then discovered the difficulty in picking the
 * number of consumers. ({@link Runtime#availableProcessors()} is not optimal when the work is I/O
 * bound; CPU cores can work on multiple tasks at once.)
 *
 * <p>Now exploring 'infinite' consumer approach where a thread is effectively created for each
 * message that comes through the queue. Similar idea to a web server that effectively creates a
 * thread for each request. (Say 'effectively' since a thread is not always created, it comes from a
 * thread pool.) This lets the OS manage the optimal number of threads (and therefore consumers) for
 * us.
 *
 * <p>Well not quite, unfortunately {@link Executors#newCachedThreadPool} leads to {@link
 * OutOfMemoryError} since it doesn't stop creating threads (similarly {@link
 * Executors#newFixedThreadPool} doesn't stop queueing tasks). Therefore, we need to cap the number
 * of tasks (and determine this optimal cap).
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
@Disabled
class QueueProcessorBenchmarkTest {

  /*
   * Statistics (run on Ryzen 9 5950x (16c32t), 10k elements)        I/O bound task  | CPU bound task
   * Sequential:                                                     767s            | 934s
   * Parallel streams (fork-join):                                   295s (2.6)      |
   * Producer-consumer, 1 Producer/1 Consumer:                       771s            |
   * Producer-consumer, 1 Producer/2 Consumers:                      381s    (2.0)   |
   * Producer-consumer, 1 Producer/4 Consumers:                      191s    (4.0)   |
   * Producer-consumer, 1 Producer/8 Consumers:                      95s     (8.1)   |
   * Producer-consumer, 1 Producer/16 Consumers:                     47.369s (16.2)  |
   * Producer-consumer, 1 Producer/32 Consumers:                     24.162s (31.7)  |
   * Producer-consumer, 1 Producer/64 Consumers:                     11.896s (64.5)  |
   * Producer-consumer, 1 Producer/128 Consumers:                    6.400s  (119.8) |
   * Producer-consumer, 1 Producer/100*CPU Consumers (3200):         1.120s  (684.8) |
   * Producer-consumer, 1 Producer/Elastic Consumer (3200 task cap): 0.889s  (862.8) |
   *
   * Conclusions:
   * CPU bound tasks speedup until the number of CPU cores TODO NOT PROVEN.
   * I/O bound tasks speedup beyond the number of CPU cores due to OS context switching.
   *
   * Threads are mapped and executed like this: Java thread -> OS thread -> CPU core.
   * (Java threads are 1-1 with OS thread (...until Project Loom).)
   * Even though I/O work blocks the Java thread, the OS unblocks and switches the OS thread
   * executing on the CPU.
   *
   * Once we have Project Loom my understanding is Java threads will be many-1 with OS threads (just
   * like OS threads are many-1 with CPU cores) and the JVM will not block but rather context switch
   * Java threads for even higher throughput of I/O bound work.
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
  void producerConsumer_100PerCpuConsumers() {
    new ProducerConsumerOrchestrator<>(() -> data(), processor())
        .run(100 * Runtime.getRuntime().availableProcessors());
  }

  @Test
  void producerConsumer_virtuallyInfiniteConsumersViaElasticThreadPool() {
    new ProducerConsumerOrchestrator<>(() -> data(), processor()).run(0);
  }

  private Stream<Integer> data() {
    // purposely make the stream unsized via iterate, then stream.parallel has a harder time
    // dividing the work (like in the actual app (traversing file tree))
    return IntStream.iterate(1, i -> i <= 10_000, i -> i + 1).boxed();
  }

  private Consumer<Integer> processor() {
    return i -> {
      ioBoundTask();
      log.info("Processed: {}", i);
    };
  }

  private void ioBoundTask() {
    try {
      Thread.sleep(Faker.instance().number().numberBetween(50, 100));
    } catch (InterruptedException e) {
      log.error("Thread interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  @SuppressFBWarnings
  private void cpuBoundTask() {
    // TODO How can we write this?
    //  Seems to get slower the more threads... memory bus bottleneck? logger writing to file?
    for (Integer i = 0; i < 1000; i++) {
      for (Integer j = 0; j < 1000; j++) {
        Integer k = i * j;
      }
    }
  }
}
