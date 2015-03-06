/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.slowoperationdetector;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ThreadUtil;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * Monitors the operation threads to find and collect data of long running operations.
 */
public final class SlowOperationDetector {

    private static final int FULL_LOG_FREQUENCY = 100;
    private static final long ONE_SECOND_IN_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final ILogger LOGGER = Logger.getLogger(SlowOperationDetector.class);

    private final ConcurrentHashMap<Integer, SlowOperationLog> slowOperationLogs
            = new ConcurrentHashMap<Integer, SlowOperationLog>();
    private final StringBuilder stackTraceStringBuilder = new StringBuilder();

    private final OperationRunner[] genericOperationRunners;
    private final OperationRunner[] partitionOperationRunners;

    private final CurrentOperationData[] genericCurrentOperationData;
    private final CurrentOperationData[] partitionCurrentOperationData;

    private final long slowOperationThresholdNanos;
    private final long logPurgeIntervalNanos;
    private final long logRetentionNanos;
    private final SlowOperationDetectorThread thread;

    public SlowOperationDetector(OperationRunner[] genericOperationRunners, OperationRunner[] partitionOperationRunners,
                                 GroupProperties groupProperties, HazelcastThreadGroup hazelcastThreadGroup) {
        this.genericOperationRunners = genericOperationRunners;
        this.partitionOperationRunners = partitionOperationRunners;

        genericCurrentOperationData = initCurrentOperationData(genericOperationRunners);
        partitionCurrentOperationData = initCurrentOperationData(partitionOperationRunners);

        slowOperationThresholdNanos = getGroupPropertyMillisAsNanos(groupProperties.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS);
        logPurgeIntervalNanos = getGroupPropertySecAsNanos(groupProperties.SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS);
        logRetentionNanos = getGroupPropertySecAsNanos(groupProperties.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS);

        thread = new SlowOperationDetectorThread(hazelcastThreadGroup);
        thread.start();
    }

    public Collection<SlowOperationLog> getSlowOperationLogs() {
        return slowOperationLogs.values();
    }

    private CurrentOperationData[] initCurrentOperationData(OperationRunner[] operationRunners) {
        CurrentOperationData[] currentOperationDataArray = new CurrentOperationData[operationRunners.length];
        for (int i = 0; i < currentOperationDataArray.length; i++) {
            currentOperationDataArray[i] = new CurrentOperationData();
            currentOperationDataArray[i].operationHashCode = -1;
        }
        return currentOperationDataArray;
    }

    private long getGroupPropertyMillisAsNanos(GroupProperties.GroupProperty groupProperty) {
        return TimeUnit.MILLISECONDS.toNanos(groupProperty.getInteger());
    }

    private long getGroupPropertySecAsNanos(GroupProperties.GroupProperty groupProperty) {
        return TimeUnit.SECONDS.toNanos(groupProperty.getInteger());
    }

    public void shutdown() {
        thread.shutdown();
    }

    public void awaitTermination(long timeoutMs) throws InterruptedException, TimeoutException {
        ThreadUtil.awaitTermination(timeoutMs, thread);
    }

    private static class CurrentOperationData {
        int operationHashCode;
        long startNanos;
        SlowOperationLog.Invocation invocation;
    }

    private class SlowOperationDetectorThread extends Thread {
        private volatile boolean running = true;

        public SlowOperationDetectorThread(HazelcastThreadGroup threadGroup) {
            super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("SlowOperationDetectorThread"));
        }

        private void shutdown() {
            running = false;
            thread.interrupt();
        }

        @Override
        public void run() {
            long lastLogPurge = System.nanoTime();
            while (running) {
                long nowNanos = System.nanoTime();
                long nowMillis = System.currentTimeMillis();

                scan(nowNanos, nowMillis, genericOperationRunners, genericCurrentOperationData);
                scan(nowNanos, nowMillis, partitionOperationRunners, partitionCurrentOperationData);

                if (purge(nowNanos, lastLogPurge)) {
                    lastLogPurge = nowNanos;
                }

                sleepInterval(nowNanos);
            }
        }

        private void scan(long nowNanos, long nowMillis, OperationRunner[] operationRunners,
                          CurrentOperationData[] currentOperationDataArray) {
            for (int i = 0; i < operationRunners.length; i++) {
                if (operationRunners[i].currentTask() == null) {
                    continue;
                }

                scanOperationRunner(nowNanos, nowMillis, operationRunners[i], currentOperationDataArray[i]);
            }
        }

        private void scanOperationRunner(long nowNanos, long nowMillis, OperationRunner operationRunner,
                                         CurrentOperationData operationData) {
            int operationHashCode = System.identityHashCode(operationRunner.currentTask());
            if (operationData.operationHashCode != operationHashCode) {
                // initialize currentOperationData for newly detected operation
                operationData.operationHashCode = operationHashCode;
                operationData.startNanos = nowNanos;
                operationData.invocation = null;
                return;
            }

            long durationNanos = nowNanos - operationData.startNanos;
            if (durationNanos < slowOperationThresholdNanos) {
                return;
            }

            SlowOperationLog.Invocation invocation = operationData.invocation;
            if (invocation != null) {
                // update existing invocation to avoid creation of stack trace
                invocation.durationNanos = TimeUnit.NANOSECONDS.toMillis(durationNanos);
                invocation.lastAccessNanos = nowNanos;
                return;
            }

            // create the stack trace once for this operation and cache the invocation for upcoming updates
            SlowOperationLog log = getOrCreateSlowOperationLog(operationRunner);
            operationData.invocation = log.getOrCreateInvocation(operationHashCode, durationNanos, nowNanos, nowMillis);

            logSlowOperation(operationRunner, log);
        }

        private SlowOperationLog getOrCreateSlowOperationLog(final OperationRunner operationRunner) {
            String prefix = "";
            for (StackTraceElement stackTraceElement : operationRunner.currentThread().getStackTrace()) {
                stackTraceStringBuilder.append(prefix).append(stackTraceElement.toString());
                prefix = "\n";
            }

            final String stackTraceString = stackTraceStringBuilder.toString();
            stackTraceStringBuilder.setLength(0);

            Integer hashCode = stackTraceString.hashCode();
            SlowOperationLog candidate = slowOperationLogs.get(hashCode);
            if (candidate != null) {
                return candidate;
            }

            candidate = new SlowOperationLog(stackTraceString, operationRunner.currentTask());
            slowOperationLogs.put(hashCode, candidate);

            return candidate;
        }

        private void logSlowOperation(OperationRunner operationRunner, SlowOperationLog log) {
            // log a warning and print the full stack trace each 100 invocations
            Object operation = operationRunner.currentTask();
            int totalInvocations = log.getTotalInvocations();
            if (totalInvocations == 1) {
                LOGGER.warning(format("Slow operation detected: %s%n%s", operation, log.getStackTrace()));
            } else {
                LOGGER.warning(format("Slow operation detected: %s (%d invocations)%n%s", operation, totalInvocations,
                        (totalInvocations % FULL_LOG_FREQUENCY == 0) ? log.getStackTrace() : log.getShortStackTrace()));
            }
        }

        private boolean purge(long nowNanos, long lastLogPurge) {
            if (nowNanos - lastLogPurge <= logPurgeIntervalNanos) {
                return false;
            }

            for (SlowOperationLog log : slowOperationLogs.values()) {
                log.purgeInvocations(nowNanos, logRetentionNanos);
                if (log.isEmpty()) {
                    slowOperationLogs.remove(log.getStackTrace().hashCode());
                }
            }
            return true;
        }

        private void sleepInterval(long nowNanos) {
            try {
                TimeUnit.NANOSECONDS.sleep(ONE_SECOND_IN_NANOS - (System.nanoTime() - nowNanos));
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
    }
}
