/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.slowoperationdetector;

import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.properties.HazelcastProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.lang.String.format;

/**
 * Monitors the {@link OperationRunner} instances of the {@link OperationExecutor} to see if operations are slow.
 * <p>
 * Slow operations are logged and can be accessed e.g. to write to a log file or report to management center.
 */
public final class SlowOperationDetector {

    private static final int FULL_LOG_FREQUENCY = 100;
    private static final long ONE_SECOND_IN_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long SLOW_OPERATION_THREAD_MAX_WAIT_TIME_TO_FINISH = TimeUnit.SECONDS.toMillis(10);

    private final ConcurrentHashMap<Integer, SlowOperationLog> slowOperationLogs
            = new ConcurrentHashMap<>();

    private final ILogger logger;

    private final long slowOperationThresholdNanos;
    private final long logPurgeIntervalNanos;
    private final long logRetentionNanos;
    private final boolean isStackTraceLoggingEnabled;

    private final OperationRunner[] genericOperationRunners;
    private final OperationRunner[] partitionOperationRunners;

    private final CurrentOperationData[] genericCurrentOperationData;
    private final CurrentOperationData[] partitionCurrentOperationData;

    private final DetectorThread detectorThread;
    private final boolean enabled;

    private boolean isFirstLog = true;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public SlowOperationDetector(LoggingService loggingServices,
                                 OperationRunner[] genericOperationRunners,
                                 OperationRunner[] partitionOperationRunners,
                                 HazelcastProperties hazelcastProperties,
                                 String hzName) {

        this.logger = loggingServices.getLogger(SlowOperationDetector.class);

        this.slowOperationThresholdNanos = hazelcastProperties.getNanos(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS);
        this.logPurgeIntervalNanos = hazelcastProperties.getNanos(SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS);
        this.logRetentionNanos = hazelcastProperties.getNanos(SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS);
        this.isStackTraceLoggingEnabled = hazelcastProperties.getBoolean(SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED);

        this.genericOperationRunners = genericOperationRunners;
        this.partitionOperationRunners = partitionOperationRunners;

        this.genericCurrentOperationData = initCurrentOperationData(genericOperationRunners);
        this.partitionCurrentOperationData = initCurrentOperationData(partitionOperationRunners);
        this.enabled = hazelcastProperties.getBoolean(SLOW_OPERATION_DETECTOR_ENABLED);
        this.detectorThread = newDetectorThread(hzName);
    }

    public List<SlowOperationDTO> getSlowOperationDTOs() {
        List<SlowOperationDTO> slowOperationDTOs = new ArrayList<SlowOperationDTO>(slowOperationLogs.size());
        for (SlowOperationLog slowOperationLog : slowOperationLogs.values()) {
            slowOperationDTOs.add(slowOperationLog.createDTO());
        }
        return slowOperationDTOs;
    }

    public void start() {
        if (enabled) {
            detectorThread.start();
        } else {
            logger.warning("The SlowOperationDetector is disabled! Slow operations will not be reported.");
        }
    }

    public void shutdown() {
        detectorThread.shutdown();
    }

    private CurrentOperationData[] initCurrentOperationData(OperationRunner[] operationRunners) {
        CurrentOperationData[] currentOperationDataArray = new CurrentOperationData[operationRunners.length];
        for (int i = 0; i < currentOperationDataArray.length; i++) {
            currentOperationDataArray[i] = new CurrentOperationData();
            currentOperationDataArray[i].operationHashCode = -1;
        }
        return currentOperationDataArray;
    }

    private DetectorThread newDetectorThread(String hzName) {
        DetectorThread thread = new DetectorThread(hzName);
        return thread;
    }

    private final class DetectorThread extends Thread {

        private final StringBuilder stackTraceStringBuilder = new StringBuilder();
        private volatile boolean running = true;

        private DetectorThread(String hzName) {
            super(createThreadName(hzName, "SlowOperationDetectorThread"));
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

                if (running) {
                    sleepInterval(nowNanos);
                }
            }
        }

        private void scan(long nowNanos, long nowMillis, OperationRunner[] operationRunners,
                          CurrentOperationData[] currentOperationDataArray) {
            for (int i = 0; i < operationRunners.length && running; i++) {
                scanOperationRunner(nowNanos, nowMillis, operationRunners[i], currentOperationDataArray[i]);
            }
        }

        private void scanOperationRunner(long nowNanos, long nowMillis, OperationRunner operationRunner,
                                         CurrentOperationData operationData) {
            Object operation = operationRunner.currentTask();
            if (operation == null) {
                return;
            }

            int operationHashCode = System.identityHashCode(operation);
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

            if (operationData.invocation != null) {
                // update existing invocation to avoid creation of stack trace
                operationData.invocation.update(nowNanos, (int) TimeUnit.NANOSECONDS.toMillis(durationNanos));
                return;
            }

            // create the stack trace once (if the running operation didn't change)
            String stackTrace = getStackTraceOrNull(operationRunner, operation);
            if (stackTrace != null) {
                // create the log for this operation and cache the invocation for upcoming updates
                SlowOperationLog log = getOrCreate(stackTrace, operation);
                int totalInvocations = log.totalInvocations.incrementAndGet();
                operationData.invocation = log.getOrCreate(operationHashCode, operation, durationNanos, nowNanos, nowMillis);

                logSlowOperation(log, totalInvocations);
            }
        }

        private String getStackTraceOrNull(OperationRunner operationRunner, Object operation) {
            StackTraceElement[] stackTraceElements = operationRunner.currentThread().getStackTrace();
            // check if the operation is still the same, if not, we can't use this stacktrace
            if (operationRunner.currentTask() != operation) {
                return null;
            }
            String prefix = "";
            for (StackTraceElement stackTraceElement : stackTraceElements) {
                stackTraceStringBuilder.append(prefix).append(stackTraceElement.toString());
                prefix = "\n\t";
            }

            String stackTrace = stackTraceStringBuilder.toString();
            stackTraceStringBuilder.setLength(0);
            return stackTrace;
        }

        private SlowOperationLog getOrCreate(String stackTrace, Object operation) {
            Integer stackTraceHashCode = stackTrace.hashCode();
            SlowOperationLog candidate = slowOperationLogs.get(stackTraceHashCode);
            if (candidate != null) {
                return candidate;
            }

            candidate = new SlowOperationLog(stackTrace, operation);
            slowOperationLogs.put(stackTraceHashCode, candidate);

            return candidate;
        }

        private void logSlowOperation(SlowOperationLog log, int totalInvocations) {
            if (isStackTraceLoggingEnabled) {
                logWithStackTrace(log, totalInvocations);
            } else if (!isFirstLog) {
                logWithoutStackTrace(log, totalInvocations);
            } else {
                logWithConfigHint(log);
            }
        }

        private void logWithStackTrace(SlowOperationLog log, int totalInvocations) {
            if (totalInvocations == 1) {
                logger.warning(format("Slow operation detected: %s%n%s", log.operation, log.stackTrace));
            } else {
                // print the full stack trace each FULL_LOG_FREQUENCY invocations
                logger.warning(format("Slow operation detected: %s (%d invocations)%n%s", log.operation, totalInvocations,
                        (totalInvocations % FULL_LOG_FREQUENCY == 0) ? log.stackTrace : log.shortStackTrace));
            }
        }

        private void logWithoutStackTrace(SlowOperationLog log, int totalInvocations) {
            if (totalInvocations == 1) {
                logger.warning(format("Slow operation detected: %s", log.operation));
            } else {
                logger.warning(format("Slow operation detected: %s (%d invocations)", log.operation, totalInvocations));
            }
        }

        private void logWithConfigHint(SlowOperationLog log) {
            // print a hint once how to enable logging of stack traces
            logger.warning(format("Slow operation detected: %s"
                            + "%nHint: You can enable the logging of stack traces with the following system property: -D%s",
                    log.operation, SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED));
            isFirstLog = false;
        }

        private boolean purge(long nowNanos, long lastLogPurge) {
            if (nowNanos - lastLogPurge <= logPurgeIntervalNanos) {
                return false;
            }

            for (SlowOperationLog log : slowOperationLogs.values()) {
                if (!running) {
                    return false;
                }
                if (log.purgeInvocations(nowNanos, logRetentionNanos)) {
                    slowOperationLogs.remove(log.stackTrace.hashCode());
                }
            }
            return true;
        }

        private void sleepInterval(long nowNanos) {
            try {
                TimeUnit.NANOSECONDS.sleep(ONE_SECOND_IN_NANOS - (System.nanoTime() - nowNanos));
            } catch (Exception ignored) {
                ignore(ignored);
            }
        }

        private void shutdown() {
            running = false;
            detectorThread.interrupt();
            try {
                detectorThread.join(SLOW_OPERATION_THREAD_MAX_WAIT_TIME_TO_FINISH);
            } catch (InterruptedException ignored) {
                currentThread().interrupt();
            }
        }
    }

    private static class CurrentOperationData {
        private int operationHashCode;
        private long startNanos;
        private SlowOperationLog.Invocation invocation;
    }
}
