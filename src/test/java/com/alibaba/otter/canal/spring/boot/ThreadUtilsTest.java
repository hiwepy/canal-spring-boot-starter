package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.util.ThreadUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

class ThreadUtilsTest {

    @Test
    void newThreadPoolExecutor() {
        ExecutorService executor = ThreadUtils.newThreadPoolExecutor(
                1, 2, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10), "test", true);
        assertThat(executor).isNotNull();
        executor.shutdown();
    }

    @Test
    void newSingleThreadExecutor() {
        ExecutorService executor = ThreadUtils.newSingleThreadExecutor("test", true);
        assertThat(executor).isNotNull();
        executor.shutdown();
    }

    @Test
    void newSingleThreadScheduledExecutor() {
        ScheduledExecutorService executor = ThreadUtils.newSingleThreadScheduledExecutor("test", true);
        assertThat(executor).isNotNull();
        executor.shutdown();
    }

    @Test
    void newFixedThreadScheduledPool() {
        ScheduledExecutorService executor = ThreadUtils.newFixedThreadScheduledPool(2, "test", true);
        assertThat(executor).isNotNull();
        executor.shutdown();
    }

    @Test
    void newThreadFactory() {
        ThreadFactory factory = ThreadUtils.newThreadFactory("test", true);
        Thread thread = factory.newThread(() -> {});
        assertThat(thread.getName()).startsWith("Remoting-test");
        assertThat(thread.isDaemon()).isTrue();
    }

    @Test
    void newGenericThreadFactoryWithName() {
        ThreadFactory factory = ThreadUtils.newGenericThreadFactory("test");
        Thread thread = factory.newThread(() -> {});
        assertThat(thread.getName()).startsWith("test");
        assertThat(thread.isDaemon()).isFalse();
    }

    @Test
    void newGenericThreadFactoryWithNameAndDaemon() {
        ThreadFactory factory = ThreadUtils.newGenericThreadFactory("test", true);
        Thread thread = factory.newThread(() -> {});
        assertThat(thread.isDaemon()).isTrue();
    }

    @Test
    void newGenericThreadFactoryWithNameAndThreads() {
        ThreadFactory factory = ThreadUtils.newGenericThreadFactory("test", 4);
        Thread thread = factory.newThread(() -> {});
        assertThat(thread.getName()).contains("test", "4");
    }

    @Test
    void newGenericThreadFactoryWithNameThreadsDaemon() {
        ThreadFactory factory = ThreadUtils.newGenericThreadFactory("test", 4, true);
        Thread thread = factory.newThread(() -> {});
        assertThat(thread.getName()).contains("test", "4");
        assertThat(thread.isDaemon()).isTrue();
    }

    @Test
    void newThread() {
        Thread thread = ThreadUtils.newThread("test", () -> {}, true);
        assertThat(thread.getName()).isEqualTo("test");
        assertThat(thread.isDaemon()).isTrue();
    }

    @Test
    void shutdownGracefullyThread() {
        Thread thread = new Thread(() -> {
            try { Thread.sleep(10000); } catch (InterruptedException e) { }
        });
        thread.start();
        ThreadUtils.shutdownGracefully(thread);
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    void shutdownGracefullyNullThread() {
        ThreadUtils.shutdownGracefully(null);
    }

    @Test
    void shutdownGracefullyExecutor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ThreadUtils.shutdownGracefully(executor, 1, TimeUnit.SECONDS);
        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    void shutdownGracefullyThreadWithMillis() {
        Thread thread = new Thread(() -> {
            try { Thread.sleep(10000); } catch (InterruptedException e) { }
        });
        thread.start();
        ThreadUtils.shutdownGracefully(thread, 100);
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    void newThreadUncaughtExceptionHandler() {
        Thread thread = ThreadUtils.newThread("test-handler", () -> {
            throw new RuntimeException("test error");
        }, false);
        thread.start();
        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            // ignore
        }
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    void shutdownGracefullyExecutorWithTimeout() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try { Thread.sleep(10000); } catch (InterruptedException e) { }
        });
        ThreadUtils.shutdownGracefully(executor, 100, TimeUnit.MILLISECONDS);
        assertThat(executor.isShutdown()).isTrue();
    }
}
