package com.willmolloy.backup.util.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Encapsulates and orchestrates {@link BlockingQueue} producer-consumer setup.
 *
 * @param <TElement> type of elements processed
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public class ProducerConsumerOrchestrator<TElement> {

  private static final Logger log = LogManager.getLogger();

  private final Supplier<Stream<TElement>> producer;
  private final Consumer<TElement> consumer;

  public ProducerConsumerOrchestrator(
      Supplier<Stream<TElement>> producer, Consumer<TElement> consumer) {
    this.producer = checkNotNull(producer);
    this.consumer = checkNotNull(consumer);
  }

  /**
   * Run the Producer/Consumer.
   *
   * @param numberOfConsumers number of consumers. 0 to use 'infinite' consumers approach via
   *     elastic thread pool
   */
  public void run(int numberOfConsumers) {
    BlockingQueue<TElement> queue = new SynchronousQueue<>();
    AtomicBoolean producerFinished = new AtomicBoolean(false);

    Thread producerThread = new Thread(new BlockingProducer<>(queue, producer), "producer");
    log.debug("Starting Producer");
    producerThread.start();

    List<Thread> consumerThreads;
    if (numberOfConsumers > 0) {
      consumerThreads =
          IntStream.range(0, numberOfConsumers)
              .mapToObj(
                  i ->
                      new Thread(
                          new BlockingConsumer<>(queue, consumer, producerFinished),
                          "consumer-%d".formatted(i)))
              .toList();
      log.debug("Starting {} Consumer(s)", numberOfConsumers);
    } else {
      consumerThreads =
          List.of(
              new Thread(
                  new BlockingElasticConsumer<>(queue, consumer, producerFinished),
                  "consumer-main"));
      log.debug("Starting Elastic Consumer");
    }
    for (Thread consumerThread : consumerThreads) {
      consumerThread.start();
    }

    try {
      // wait until producer finishes
      producerThread.join();
      // signal completion
      producerFinished.set(true);
      // wait until consumers finish
      for (Thread consumerThread : consumerThreads) {
        consumerThread.join();
      }
    } catch (InterruptedException e) {
      log.warn("Producer/Consumer interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private static final class BlockingProducer<TElement> implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final BlockingQueue<TElement> queue;
    private final Supplier<Stream<TElement>> producer;

    private BlockingProducer(BlockingQueue<TElement> queue, Supplier<Stream<TElement>> producer) {
      this.queue = queue;
      this.producer = producer;
    }

    @Override
    public void run() {
      int count = 0;
      try {
        Iterator<TElement> iterator = producer.get().iterator();
        while (iterator.hasNext()) {
          queue.put(iterator.next());
          count++;
        }
      } catch (InterruptedException e) {
        log.warn("Producer interrupted", e);
        Thread.currentThread().interrupt();
      } finally {
        log.debug("Produced {} elements(s)", count);
      }
    }
  }

  private static final class BlockingConsumer<TElement> implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final BlockingQueue<TElement> queue;
    private final Consumer<TElement> consumer;
    private final AtomicBoolean producerFinished;

    private BlockingConsumer(
        BlockingQueue<TElement> queue,
        Consumer<TElement> consumer,
        AtomicBoolean producerFinished) {
      this.queue = queue;
      this.consumer = consumer;
      this.producerFinished = producerFinished;
    }

    @Override
    public void run() {
      int count = 0;
      try {
        while (!producerFinished.get()) {
          // need timeout in case the last element is consumed before producer finished is signalled
          TElement element = queue.poll(500, TimeUnit.MILLISECONDS);
          if (element != null) {
            consumer.accept(element);
            count++;
          }
        }
      } catch (InterruptedException e) {
        log.warn("Consumer interrupted", e);
        Thread.currentThread().interrupt();
      } finally {
        log.debug("Consumed {} elements(s)", count);
      }
    }
  }

  /**
   * This consumer submits each element that comes through the queue to an elastic thread pool for
   * processing asynchronously.
   *
   * <p>Therefore, it grows infinitely (well, until a fixed number of concurrent tasks) and uses the
   * CPU better when tasks are I/O bound.
   *
   * @param <TElement> type of elements consumed
   */
  private static final class BlockingElasticConsumer<TElement> implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final BlockingQueue<TElement> queue;
    private final Consumer<TElement> consumer;
    private final AtomicBoolean producerFinished;

    private BlockingElasticConsumer(
        BlockingQueue<TElement> queue,
        Consumer<TElement> consumer,
        AtomicBoolean producerFinished) {
      this.queue = queue;
      this.consumer = consumer;
      this.producerFinished = producerFinished;
    }

    @Override
    public void run() {
      AtomicInteger threadCount = new AtomicInteger();
      // alright, it's not infinite, we are fixing the number of concurrent tasks to avoid
      // OutOfMemoryError
      // have found CPU*100 works well
      // (it's still better than CPU*100 consumers, since this approach is elastic and starts work
      // immediately, also only need one thread reading the queue)
      Executor threadPool =
          new BlockingElasticExecutor(
              Runtime.getRuntime().availableProcessors() * 100,
              runnable ->
                  new Thread(
                      runnable, "consumer-worker-%d".formatted(threadCount.getAndIncrement())));

      // TODO this ArrayList is a memory leak
      //  better way to signal when unknown number of tasks is complete?
      List<Future<?>> tasks = new ArrayList<>();

      try {
        while (!producerFinished.get()) {
          // need timeout in case the last element is consumed before producer finished is signalled
          TElement element = queue.poll(500, TimeUnit.MILLISECONDS);
          if (element != null) {
            CompletableFuture<?> task =
                CompletableFuture.runAsync(() -> consumer.accept(element), threadPool);
            tasks.add(task);
          }
        }

        for (Future<?> task : tasks) {
          task.get();
        }
      } catch (InterruptedException e) {
        log.warn("Consumer interrupted", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        log.warn("Async consumer processing failed", e);
      } finally {
        log.debug("Consumed {} elements(s)", tasks.size());
        log.debug("Created {} thread(s)", threadCount.get());
      }
    }

    /**
     * This executor achieves the benefit of both {@link Executors#newCachedThreadPool} and {@link
     * Executors#newFixedThreadPool}.
     *
     * <p>Instead of infinitely creating threads or queueing tasks (and getting {@link
     * OutOfMemoryError}) it blocks when the concurrent task limit is reached. Furthermore, it
     * caches the threads unlike {@link Executors#newFixedThreadPool}.
     */
    // https://www.baeldung.com/java-executors-cached-fixed-threadpool#unfortunate-similarities
    // code from https://stackoverflow.com/a/50361304/6122976
    private static final class BlockingElasticExecutor implements Executor {

      private static final Logger log = LogManager.getLogger();

      private final Semaphore semaphore;
      private final Executor delegate;

      private BlockingElasticExecutor(int numberOfConcurrentTasks, ThreadFactory threadFactory) {
        this.semaphore = new Semaphore(numberOfConcurrentTasks);
        delegate = Executors.newCachedThreadPool(threadFactory);
      }

      @Override
      public void execute(Runnable command) {
        try {
          semaphore.acquire();
          delegate.execute(
              () -> {
                try {
                  command.run();
                } finally {
                  semaphore.release();
                }
              });
        } catch (InterruptedException e) {
          log.error("Interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
