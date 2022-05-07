package com.willmolloy.backup.util.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * @param <TElement> type of elements produced/consumed.
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public class ProducerConsumerOrchestrator<TElement> {

  private static final Logger log = LogManager.getLogger();

  private static final int BUFFER_SIZE = 10;

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
   * @param numberOfConsumers number of consumers. 0 to use 'unlimited' consumers approach via
   *     cached thread pool
   */
  public void run(int numberOfConsumers) {
    ArrayBlockingQueue<TElement> queue = new ArrayBlockingQueue<>(BUFFER_SIZE);
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
                  new BlockingUnboundedConsumer<>(queue, consumer, producerFinished),
                  "consumer-main"));
      log.debug("Starting Unbounded Consumer");
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

  private static final class BlockingProducer<TData> implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final BlockingQueue<TData> queue;
    private final Supplier<Stream<TData>> producer;

    private BlockingProducer(BlockingQueue<TData> queue, Supplier<Stream<TData>> producer) {
      this.queue = queue;
      this.producer = producer;
    }

    @Override
    public void run() {
      int count = 0;
      try {
        Iterator<TData> iterator = producer.get().iterator();
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

  private static final class BlockingConsumer<TData> implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final BlockingQueue<TData> queue;
    private final Consumer<TData> consumer;
    private final AtomicBoolean producerFinished;

    private BlockingConsumer(
        BlockingQueue<TData> queue, Consumer<TData> consumer, AtomicBoolean producerFinished) {
      this.queue = queue;
      this.consumer = consumer;
      this.producerFinished = producerFinished;
    }

    @Override
    public void run() {
      int count = 0;
      try {
        while (!producerFinished.get() || !queue.isEmpty()) {
          TData data = queue.poll(1, TimeUnit.SECONDS);
          if (data != null) {
            consumer.accept(data);
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

  // This consumer submits each element that comes through the queue to a
  // {@link Executors#newCachedThreadPool} for processing asynchronously.
  // Therefore, it grows unbounded and (in theory) should use the hardware most optimally as it
  // delegates the thread management to the OS.
  private static final class BlockingUnboundedConsumer<TData> implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final BlockingQueue<TData> queue;
    private final Consumer<TData> consumer;
    private final AtomicBoolean producerFinished;

    private BlockingUnboundedConsumer(
        BlockingQueue<TData> queue, Consumer<TData> consumer, AtomicBoolean producerFinished) {
      this.queue = queue;
      this.consumer = consumer;
      this.producerFinished = producerFinished;
    }

    @Override
    public void run() {
      AtomicInteger count = new AtomicInteger();
      try {
        ExecutorService threadPool =
            Executors.newCachedThreadPool(
                runnable -> new Thread(runnable, "consumer-%d".formatted(count.getAndIncrement())));
        // TODO this ArrayList is a memory leak, better way to signal consumer is finished?
        List<Future<?>> tasks = new ArrayList<>();

        while (!producerFinished.get() || !queue.isEmpty()) {
          TData data = queue.poll(1, TimeUnit.SECONDS);
          if (data != null) {
            Future<?> task = threadPool.submit(() -> consumer.accept(data));
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
        log.warn("Consumer failed to process data", e);
      } finally {
        log.debug("Consumed {} elements(s)", count.get());
      }
    }
  }
}
