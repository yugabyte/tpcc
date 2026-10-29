package com.oltpbenchmark.api;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.apache.log4j.Logger;

/**
 * Keeps repeated worker failures from flooding the log. With thousands of terminals, a node loss makes
 * every terminal fail at about the same time. The first failure of each SQLState is logged as a WARN
 * immediately; later ones are only counted, and a background thread logs one summary WARN per SQLState
 * every REPORT_INTERVAL_SECS. Every occurrence still goes to DEBUG.
 */
public final class FailureLogReporter {
    private static final Logger LOG = Logger.getLogger(FailureLogReporter.class);

    static final long REPORT_INTERVAL_SECS = 30;

    public enum Category {
        CONNECTION_ACQUIRE("connection acquire failures"),
        UNEXPECTED_SQL("unexpected transaction failures");

        private final String description;

        Category(String description) {
            this.description = description;
        }
    }

    private static final class Counter {
        // Counts are approximate: increments that race with sumThenReset() may be lost.
        final LongAdder count = new LongAdder();
        final AtomicReference<String> sample = new AtomicReference<>();
    }

    private static final Map<Category, ConcurrentHashMap<String, Counter>> counters = new EnumMap<>(Category.class);
    static {
        for (Category category : Category.values()) {
            counters.put(category, new ConcurrentHashMap<>());
        }
    }

    private static final Object reportLock = new Object();
    private static volatile boolean logEveryOccurrence = false;
    private static Thread reporterThread;

    private FailureLogReporter() {
    }

    /**
     * When enabled, every failure is logged as a WARN with its exact message and nothing is counted or
     * summarized.
     */
    public static void setLogEveryOccurrence(boolean enabled) {
        logEveryOccurrence = enabled;
    }

    /**
     * Records one failure. The message is only built when it is actually logged or sampled, so
     * suppressed failures on the hot failure path stay cheap.
     */
    public static void record(Category category, String sqlState, Supplier<String> message) {
        if (logEveryOccurrence) {
            LOG.warn(message.get());
            return;
        }

        String key = String.valueOf(sqlState);
        ConcurrentHashMap<String, Counter> byState = counters.get(category);
        // Fast path once this SQLState has been seen. putIfAbsent only runs for the first failure.
        Counter counter = byState.get(key);
        if (counter == null) {
            // null means this thread inserted the counter (first failure). Non-null means another
            // thread won the race and we should count against that counter instead.
            Counter existing = byState.putIfAbsent(key, new Counter());
            if (existing == null) {
                LOG.warn(String.format("%s (summarized every %ds)", message.get(), REPORT_INTERVAL_SECS));
                return;
            }
            counter = existing;
        }

        counter.count.increment();
        String msg = null;
        // Only the first failure of each interval writes the sample, so thousands of failing
        // terminals mostly read this field instead of all writing it.
        if (counter.sample.get() == null) {
            msg = message.get();
            counter.sample.compareAndSet(null, msg);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(msg != null ? msg : message.get());
        }
    }

    public static synchronized void start() {
        if (reporterThread != null) {
            return;
        }
        reporterThread = new Thread(FailureLogReporter::runLoop, "FailureLogReporter");
        reporterThread.setDaemon(true);
        reporterThread.start();
    }

    /**
     * Stops the reporter thread and logs whatever was counted since the last summary.
     */
    public static synchronized void stop() {
        if (reporterThread == null) {
            return;
        }
        reporterThread.interrupt();
        try {
            reporterThread.join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        reporterThread = null;
        report();
    }

    private static void runLoop() {
        while (true) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(REPORT_INTERVAL_SECS));
            } catch (InterruptedException ex) {
                return;
            }
            report();
        }
    }

    private static void report() {
        // Two concurrent sumThenReset() calls on the same adder could report the same increments twice.
        // This is not the class monitor: stop() holds that while joining the reporter thread.
        synchronized (reportLock) {
            for (Map.Entry<Category, ConcurrentHashMap<String, Counter>> byCategory : counters.entrySet()) {
                for (Map.Entry<String, Counter> byState : byCategory.getValue().entrySet()) {
                    Counter counter = byState.getValue();
                    long count = counter.count.sumThenReset();
                    String sample = counter.sample.getAndSet(null);
                    if (count > 0) {
                        LOG.warn(String.format("%d more %s [SQLState=%s]%s",
                                               count, byCategory.getKey().description, byState.getKey(),
                                               sample != null ? ": " + sample : ""));
                    }
                }
            }
        }
    }
}
