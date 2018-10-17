/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.test;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.io.Closeable;

import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * Utility to monitor performance in tests.
 *
 * This is not meant to replace proper benchmarking, but to provide a quick feedback
 * while changing parts of Hazelcast. It can also be used to check a test is making any progress at all.
 *
 * Internally it uses a histogram and it can provide both throughput and latency numbers.
 *
 * The monitor has to be explicitly closed otherwise it will leave a dangling thread.
 *
 * The monitor is thread-safe in a sense multiple threads can use a single instance of the monitor to record
 * performance data. However only a single (arbitrary) thread can close the monitor.
 *
 * Possible improvement: Introduce a concept of warm-up.
 *
 */
public final class PerformanceMonitor implements Closeable {
    private final static long MONITORING_DISABLED = -1;
    private final static int HISTOGRAM_NUMBER_OF_SIGNIFICANT_DIGITS = 5;

    private final Thread monitoringThread;
    private final Histogram deltaHistogram = new Histogram(HISTOGRAM_NUMBER_OF_SIGNIFICANT_DIGITS);
    private final Histogram totalHistogram = new Histogram(HISTOGRAM_NUMBER_OF_SIGNIFICANT_DIGITS);
    private final String name;
    private final long monitoringIntervalMillis;

    private Recorder recorder = new Recorder(HISTOGRAM_NUMBER_OF_SIGNIFICANT_DIGITS);
    private ThreadLocal<Long> lastIteration = new ThreadLocal<Long>();

    private volatile boolean stopRequested;

    /**
     * Create a new performance monitor.
     *
     * @param monitorName name of the monitor. The name is used a prefix when dumping data.
     * @param monitoringIntervalMillis how often to dump performance data. It can be -1 to disable it.
     */
    public PerformanceMonitor(String monitorName, long monitoringIntervalMillis) {
        checkHasText(monitorName, "missing monitor name");
        if (monitoringIntervalMillis <= 0) {
            if (monitoringIntervalMillis != MONITORING_DISABLED) {
                throw new IllegalArgumentException("Monitoring interval must be either a positive integer or "
                        + MONITORING_DISABLED + " to disable monitoring altogether");
            }
        }

        this.name = monitorName;
        this.monitoringIntervalMillis = monitoringIntervalMillis;
        monitoringThread = new MonitoringThread();
        if (monitoringIntervalMillis > 0) {
            monitoringThread.start();
        }
    }

    /**
     * Indicate an iteration finished. Latency is automatically calculated as an elapsed time between 2 subsequent
     * calls made by the same thread. This is the simplest way to use the performance monitor, it's well suitable
     * for testing blocking calls.
     *
     */
    public void iterationFinished() {
        long now = System.nanoTime();
        Long lastIterationSnapshot = lastIteration.get();
        lastIteration.set(now);
        if (lastIterationSnapshot == null) {
            return;
        }

        long deltaNanos = now - lastIterationSnapshot;
        recorder.recordValue(deltaNanos);
    }

    /**
     * Record externally measured latency. This is an alternative to {@link #iterationFinished()}, it's a bit more
     * complicated to use - as the user code is responsible for measuring latency - but it's more flexible.
     *
     * The most common use-case: measuring performance of async calls.
     *
     * @param latencyNanos latency in nanos to be recorded.
     */
    public void recordLatency(long latencyNanos) {
        recorder.recordValue(latencyNanos);
    }

    /**
     * Close the monitor and stop monitoring thread.
     *
     */
    @Override
    public void close() {
        if (monitoringIntervalMillis == MONITORING_DISABLED) {
            return;
        }
        if (stopRequested) {
            return;
        }
        stopRequested = true;
        monitoringThread.interrupt();
        try {
            monitoringThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for monitoring thread to finish");
        }
    }

    /**
     * Close the monitor, stop the monitoring thread and return a histogram with all recorded values.
     *
     * @return histogram will all recorded values
     */
    public Histogram closeAndGetHistogram() {
        close();
        updateHistograms();
        return totalHistogram;
    }

    private class MonitoringThread extends Thread {
        @Override
        public void run() {
            final long startedTimestamp = System.nanoTime();
            long lastTimestamp = startedTimestamp;
            while (!stopRequested) {
                waitForNextInterval();
                long now = System.nanoTime();
                long totalElapsedNanos = now - startedTimestamp;
                long deltaElapsedNanos = now - lastTimestamp;
                lastTimestamp = now;
                updateHistograms();
                printPerfData(totalElapsedNanos, deltaElapsedNanos);
            }
        }

        private void waitForNextInterval() {
            try {
                Thread.sleep(monitoringIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void printPerfData(double totalElapsedNanos, double deltaElapsedNanos) {
        long totalCount = totalHistogram.getTotalCount();
        long deltaCount = deltaHistogram.getTotalCount();
        double totalThroughput = ((double)totalCount) / (totalElapsedNanos / 1000000000);
        double deltaThroughput = ((double)deltaCount) / (deltaElapsedNanos / 1000000000);

        long percentile0_5 = deltaHistogram.getValueAtPercentile(.5);
        long percentile0_8 = deltaHistogram.getValueAtPercentile(.8);
        long percentile0_9 = deltaHistogram.getValueAtPercentile(.9);
        long percentile0_99 = deltaHistogram.getValueAtPercentile(.99);
        long maxValue = deltaHistogram.getMaxValue();

        System.out.printf(name + ": Total throughput: %,10.1f ops/s, Delta throughput: %,10.1f ops/s, "
                + "Total count: %8s ops, Delta count: %8s ops %n",
                totalThroughput, deltaThroughput, totalCount, deltaCount);
        System.out.printf(name + ": Delta percentiles: 0.5: %,d ns, 0.8: %,d ns, 0.9: %,d ns, 0.99: %,d ns, max: %,d ns %n",
                percentile0_5, percentile0_8, percentile0_9, percentile0_99, maxValue);
    }

    private void updateHistograms() {
        recorder.getIntervalHistogramInto(deltaHistogram);
        totalHistogram.add(deltaHistogram);
    }
}
