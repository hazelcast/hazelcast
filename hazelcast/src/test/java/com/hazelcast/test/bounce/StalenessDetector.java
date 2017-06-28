package com.hazelcast.test.bounce;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.bounce.BounceMemberRule.STALENESS_DETECTOR_DISABLED;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StalenessDetector {
    private final long maximumStaleNanos;
    private final List<BounceMemberRule.TestTaskRunable> tasks = new ArrayList<BounceMemberRule.TestTaskRunable>();

    public StalenessDetector(long maximumStaleSeconds) {
        this.maximumStaleNanos = maximumStaleSeconds == STALENESS_DETECTOR_DISABLED
                ? maximumStaleSeconds
                : SECONDS.toNanos(maximumStaleSeconds);
    }

    public void registerTask(Runnable task) {
        if (maximumStaleNanos == STALENESS_DETECTOR_DISABLED) {
            return;
        }
        if (task instanceof BounceMemberRule.TestTaskRunable) {
            tasks.add((BounceMemberRule.TestTaskRunable)task);
        } else {
            throw new UnsupportedOperationException("Staleness checking is enabled only for automatically repeated tests");
        }
    }

    public void assertProgress() {
        long now = System.nanoTime();
        for (BounceMemberRule.TestTaskRunable task : tasks) {
            long lastIterationStartedTimestamp = task.getLastIterationStartedTimestamp();
            if (lastIterationStartedTimestamp == 0) {
                //the tasks haven't started yet
                continue;
            }
            long currentStaleNanos = now - lastIterationStartedTimestamp;
            if (currentStaleNanos > maximumStaleNanos) {
                onStalenessDetected(task, currentStaleNanos);
            }
        }
    }

    private void onStalenessDetected(BounceMemberRule.TestTaskRunable task, long currentStaleNanos) {
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
