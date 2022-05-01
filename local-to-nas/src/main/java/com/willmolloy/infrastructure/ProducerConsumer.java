package com.willmolloy.infrastructure;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

  private static final int BUFFER_SIZE = 100;
  private static final int NUM_CONSUMERS = Runtime.getRuntime().availableProcessors();

  private final Supplier<Stream<TData>> producer;
  private final Consumer<TData> consumer;

  public ProducerConsumer(Supplier<Stream<TData>> producer, Consumer<TData> consumer) {
    this.producer = producer;
    this.consumer = consumer;
  }

  /**
   * Run the Producer/Consumer.
   *
   * @throws InterruptedException if interrupted waiting for the producer
   */
  public void run() throws InterruptedException {
    ArrayBlockingQueue<TData> queue = new ArrayBlockingQueue<>(BUFFER_SIZE);

    Thread producerThread = new Thread(new BlockingProducer<>(queue, producer), "producer");
    List<Thread> consumerThreads =
        IntStream.range(0, NUM_CONSUMERS)
            .mapToObj(
                i ->
                    new Thread(new BlockingConsumer<>(queue, consumer), "consumer-%d".formatted(i)))
            .toList();

    producerThread.start();
    consumerThreads.forEach(Thread::start);

    // wait until producer stops producing
    producerThread.join();

    // kill all consumers
    consumerThreads.forEach(Thread::interrupt);
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
                    log.error("Producer interrupted", e);
                  }
                });
      } finally {
        log.info("Produced {} node(s)", count.get());
      }
    }
  }

  private static final class BlockingConsumer<TData> implements Runnable {

    private final BlockingQueue<TData> queue;
    private final Consumer<TData> consumer;

    private BlockingConsumer(BlockingQueue<TData> queue, Consumer<TData> consumer) {
      this.queue = queue;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      while (true) {
        try {
          TData data = queue.take();
          consumer.accept(data);
        } catch (InterruptedException e) {
          // expected
          return;
        }
      }
    }
  }
}
