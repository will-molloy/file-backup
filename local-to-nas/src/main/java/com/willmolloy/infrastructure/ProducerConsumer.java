package com.willmolloy.infrastructure;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
 * Encapsulates {@link BlockingQueue} producer/consumer setup.
 *
 * @param <TData> type of data produced/consumed.
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
public class ProducerConsumer<TData> {

  private static final Logger log = LogManager.getLogger();

  private static final int NUM_CONSUMERS = Runtime.getRuntime().availableProcessors() / 2;
  private static final int BUFFER_SIZE = NUM_CONSUMERS * 100;

  private final Supplier<Stream<TData>> producer;
  private final Consumer<TData> consumer;

  public ProducerConsumer(Supplier<Stream<TData>> producer, Consumer<TData> consumer) {
    this.producer = producer;
    this.consumer = consumer;
  }

  /** Run the Producer/Consumer. */
  public void run() {
    ArrayBlockingQueue<TData> queue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    AtomicBoolean producerFinished = new AtomicBoolean(false);

    Thread producerThread = new Thread(new BlockingProducer<>(queue, producer), "producer");
    List<Thread> consumerThreads =
        IntStream.range(0, NUM_CONSUMERS)
            .mapToObj(
                i ->
                    new Thread(
                        new BlockingConsumer<>(queue, consumer, producerFinished),
                        "consumer-%d".formatted(i)))
            .toList();

    producerThread.start();
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
      AtomicInteger count = new AtomicInteger();
      try {
        producer
            .get()
            .forEach(
                data -> {
                  try {
                    queue.put(data);
                    count.getAndIncrement();
                  } catch (InterruptedException e) {
                    log.warn("Producer interrupted", e);
                    Thread.currentThread().interrupt();
                  }
                });
      } finally {
        log.debug("Produced {} node(s)", count.get());
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
      AtomicInteger count = new AtomicInteger();
      try {
        while (!producerFinished.get() || !queue.isEmpty()) {
          try {
            TData data = queue.poll(1, TimeUnit.SECONDS);
            if (data != null) {
              consumer.accept(data);
              count.getAndIncrement();
            }
          } catch (InterruptedException e) {
            log.warn("Consumer interrupted", e);
            Thread.currentThread().interrupt();
          }
        }
      } finally {
        log.debug("Consumed {} node(s)", count.get());
      }
    }
  }
}
