package com.willmolloy.backup.util.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.ForwardingExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Decorates {@link ExecutorService} as {@link AutoCloseable}.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
// TODO don't need with JDK 19
class CloseableExecutorService extends ForwardingExecutorService implements AutoCloseable {

  private final ExecutorService delegate;

  CloseableExecutorService(ExecutorService delegate) {
    this.delegate = checkNotNull(delegate);
  }

  @Override
  protected ExecutorService delegate() {
    return delegate;
  }

  @Override
  public void close() {
    // copy paste from JDK 19 EA
    boolean terminated = isTerminated();
    if (!terminated) {
      shutdown();
      boolean interrupted = false;
      while (!terminated) {
        try {
          terminated = awaitTermination(1L, TimeUnit.DAYS);
        } catch (InterruptedException e) {
          if (!interrupted) {
            shutdownNow();
            interrupted = true;
          }
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
