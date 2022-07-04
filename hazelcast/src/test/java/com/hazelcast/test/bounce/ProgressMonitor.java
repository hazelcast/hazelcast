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

package com.hazelcast.test.bounce;

import com.hazelcast.internal.util.Timer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.bounce.BounceMemberRule.STALENESS_DETECTOR_DISABLED;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ProgressMonitor {
    private static final long PROGRESS_LOGGING_INTERVAL_NANOS = SECONDS.toNanos(5);
    private static final ILogger LOGGER = Logger.getLogger(ProgressMonitor.class);

    private final long maximumStaleNanos;
    private final List<BounceMemberRule.TestTaskRunnable> tasks = new ArrayList<BounceMemberRule.TestTaskRunnable>();

    private long lastProgressLoggedNanos;
    private long progressDelta;

    public ProgressMonitor(long maximumStaleSeconds) {
        this.maximumStaleNanos = maximumStaleSeconds == STALENESS_DETECTOR_DISABLED
                ? maximumStaleSeconds
                : SECONDS.toNanos(maximumStaleSeconds);
    }

    public void registerTask(Runnable task) {
        if (task instanceof BounceMemberRule.TestTaskRunnable) {
            tasks.add((BounceMemberRule.TestTaskRunnable) task);
        } else if (maximumStaleNanos != STALENESS_DETECTOR_DISABLED) {
            throw new UnsupportedOperationException("Progress checking is enabled only for automatically repeated tasks");
        }
    }

    public void checkProgress() {
        long startNanos = Timer.nanos();
        long aggregatedProgress = 0;
        long maxLatencyNanos = 0;
        for (BounceMemberRule.TestTaskRunnable task : tasks) {
            long lastIterationStartedTimestamp = task.getLastIterationStartedTimestamp();
            if (lastIterationStartedTimestamp == 0) {
                //the tasks haven't started yet
                continue;
            }
            aggregatedProgress += task.getIterationCounter();
            maxLatencyNanos = Math.max(maxLatencyNanos, task.getMaxLatencyNanos());
            long currentStaleNanos = startNanos - lastIterationStartedTimestamp;
            if (currentStaleNanos > maximumStaleNanos) {
                onStalenessDetected(task, currentStaleNanos);
            }
        }
        logProgress(startNanos, aggregatedProgress, maxLatencyNanos);
    }

    private void logProgress(long now, long aggregatedProgress, long maxLatencyNanos) {
        if (now > lastProgressLoggedNanos + PROGRESS_LOGGING_INTERVAL_NANOS) {
            StringBuilder sb = new StringBuilder("Aggregated progress: ")
                    .append(aggregatedProgress)
                    .append(" operations. ");
            progressDelta = aggregatedProgress - progressDelta;
            if (lastProgressLoggedNanos > 0) {
                sb.append("Maximum latency: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(maxLatencyNanos))
                        .append(" ms.");

                long timeInNanos = now - lastProgressLoggedNanos;
                double timeInSeconds = (double) timeInNanos / 1000000000;
                double progressPerSecond = progressDelta / timeInSeconds;
                sb.append("Throughput in last ")
                        .append((long) (timeInSeconds * 1000))
                        .append(" ms: ")
                        .append((long) progressPerSecond)
                        .append(" ops / second. ");

            }

            lastProgressLoggedNanos = now;
            LOGGER.info(sb.toString());
        }
    }

    private void onStalenessDetected(BounceMemberRule.TestTaskRunnable task, long currentStaleNanos) {
        // this could seems redundant as the Hazelcast JUnit runner will also take threadumps
        // however in this case we are doing the threaddump before declaring a test failure
        // and stopping Hazelcast instances -> there is a higher chance we will actually
        // record something useful.
        long currentStaleSeconds = NANOSECONDS.toSeconds(currentStaleNanos);
        long maximumStaleSeconds = NANOSECONDS.toSeconds(maximumStaleNanos);
        StringBuilder sb = new StringBuilder("Stalling task detected: ")
                .append(task).append('\n')
                .append("Maximum staleness allowed (in seconds): ").append(maximumStaleSeconds).append('\n')
                .append(", Current staleness detected (in seconds): ").append(currentStaleSeconds).append('\n');
        Thread taskThread = task.getCurrentThreadOrNull();
        appendStackTrace(taskThread, sb);
        throw new AssertionError(sb.toString());
    }

    private void appendStackTrace(Thread thread, StringBuilder sb) {
        if (thread == null) {
            sb.append("The task has no thread currently assigned!");
            return;
        }

        sb.append("Assigned thread: ").append(thread).append(", Stacktrace: \n");
        StackTraceElement[] stackTraces = thread.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTraces) {
            sb.append("**").append(stackTraceElement).append('\n');
        }
        sb.append("**********");
    }
}
