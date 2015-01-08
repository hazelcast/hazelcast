package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.util.EmptyStatement;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

/**
 * The CleanupThread deals with operations that need to be re-invoked.
 * <p/>
 * It periodically iterates over all invocations in this BasicOperationService and calls the
 * {@link Invocation#handleOperationTimeout()}
 * {@link Invocation#handleBackupTimeout(long)} methods.
 * This gives each invocation the opportunity to handle with an operation (especially required for async ones)
 * and/or a backup not completing in time.
 * <p/>
 * The previous approach was that for each BackupAwareOperation a task was scheduled to deal with the timeout. The problem
 * is that the actual operation already could be completed, but the task is still scheduled and this can lead to an OOME.
 * Apart from that it also had quite an impact on performance since there is more interaction with concurrent data-structures
 * (e.g. the priority-queue of the scheduled-executor).
 * <p/>
 * We use a dedicated thread instead of a shared ScheduledThreadPool because there will not be that many of these threads
 * (each member-HazelcastInstance gets 1) and we don't want problems in 1 member causing problems in the other.
 */
final class CleanupThread extends Thread {

    public static final int DELAY_MILLIS = 1000;

    private volatile boolean shutdown;
    private final OperationServiceImpl operationService;
    private final ILogger logger;

    CleanupThread(OperationServiceImpl operationService) {
        super(operationService.node.getHazelcastThreadGroup().getThreadNamePrefix("CleanupThread"));
        this.logger = operationService.logger;
        this.operationService = operationService;
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                scanHandleOperationTimeout();
                if (!shutdown) {
                    sleep();
                }
            }
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe("Failed to run", t);
        }
    }

    private void sleep() {
        try {
            Thread.sleep(DELAY_MILLIS);
        } catch (InterruptedException ignore) {
            // can safely be ignored. If this thread wants to shut down, we'll read the shutdown variable.
            EmptyStatement.ignore(ignore);
        }
    }

    private void scanHandleOperationTimeout() {
        if (operationService.invocations.isEmpty()) {
            return;
        }

        for (Invocation invocation : operationService.invocations.values()) {
            if (shutdown) {
                return;
            }
            try {
                invocation.handleOperationTimeout();
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to handle operation timeout of invocation:" + invocation, t);
            }
            try {
                invocation.handleBackupTimeout(operationService.backupOperationTimeoutMillis);
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to handle backup timeout of invocation:" + invocation, t);
            }
        }
    }
}
