package com.willmolloy.backup.util.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
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

  /** Run the Producer/Consumer. */
  public void run() {
    BlockingQueue<TElement> queue = new SynchronousQueue<>();
    AtomicBoolean producerFinished = new AtomicBoolean(false);

    Thread producerThread = new Thread(new BlockingProducer<>(queue, producer), "producer");
    log.debug("Starting Producer");
    producerThread.start();

    Thread mainConsumerThread =
        new Thread(
            new BlockingElasticConsumer<>(queue, consumer, producerFinished), "consumer-main");
    log.debug("Starting Elastic Consumer");
    mainConsumerThread.start();

    try {
      // wait until producer finishes
      producerThread.join();
      // signal completion
      producerFinished.set(true);
      // wait until consumers finish
      mainConsumerThread.join();
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
      this.queue = checkNotNull(queue);
      this.producer = checkNotNull(producer);
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

  /**
   * This consumer submits each element that comes through the queue to a thread pool for processing
   * asynchronously.
   *
   * <p>Therefore, it grows infinitely and uses the CPU better when tasks are I/O bound (compared to
   * the manual process of calculating and tweaking settings to find the optimal number of
   * consumers).
   *
   * <p>Can lead to {@link OutOfMemoryError} in theory.
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
      this.queue = checkNotNull(queue);
      this.consumer = checkNotNull(consumer);
      this.producerFinished = checkNotNull(producerFinished);
    }

    @Override
    public void run() {
      ExecutorService threadPool = threadPool();

      // TODO this ArrayList is a memory leak
      List<Future<?>> tasks = new ArrayList<>(1_000_000);

      try {
        while (!producerFinished.get()) {
          // need timeout in case the last element is consumed before producer finished is signalled
          TElement element = queue.poll(500, TimeUnit.MILLISECONDS);
          if (element != null) {
            Future<?> task = threadPool.submit(() -> consumer.accept(element));
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
      }
    }

    private ExecutorService threadPool() {
      // TODO use virtual threads
      AtomicInteger threadCount = new AtomicInteger();
      return Executors.newCachedThreadPool(
          runnable ->
              new Thread(runnable, "consumer-worker-%d".formatted(threadCount.getAndIncrement())));
    }
  }
}
