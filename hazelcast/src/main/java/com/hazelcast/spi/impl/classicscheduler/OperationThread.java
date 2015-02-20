package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.OperationHandler;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

/**
 * The OperationThread is responsible for processing operations, packets containing operations and runnable's.
 * <p/>
 * There are 2 flavors of OperationThread:
 * - threads that deal with operations for a specific partition
 * - threads that deal with non partition specific tasks
 * <p/>
 * The actual processing of an operation is forwarded to the {@link OperationHandler}.
 */
public abstract class OperationThread extends HazelcastManagedThread {

    final int threadId;
    final ScheduleQueue scheduleQueue;
    // This field is updated by this OperationThread (so a single writer) and can be read by other threads.
    volatile long processedCount;

    private final NodeExtension nodeExtension;
    private final ILogger logger;
    private volatile boolean shutdown;

    private OperationHandler currentOperationHandler;

    public OperationThread(String name, int threadId, ScheduleQueue scheduleQueue,
                           ILogger logger, HazelcastThreadGroup threadGroup, NodeExtension nodeExtension) {
        super(threadGroup.getInternalThreadGroup(), name);
        setContextClassLoader(threadGroup.getClassLoader());
        this.scheduleQueue = scheduleQueue;
        this.threadId = threadId;
        this.logger = logger;
        this.nodeExtension = nodeExtension;
    }

    public OperationHandler getCurrentOperationHandler() {
        return currentOperationHandler;
    }

    public abstract OperationHandler getOperationHandler(int partitionId);

    @Override
    public final void run() {
        nodeExtension.onThreadStart(this);
        try {
            doRun();
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe(t);
        } finally {
            nodeExtension.onThreadStop(this);
        }
    }

    private void doRun() {
        for (; ; ) {
            Object task;
            try {
                task = scheduleQueue.take();
            } catch (InterruptedException e) {
                if (shutdown) {
                    return;
                }
                continue;
            }

            if (shutdown) {
                return;
            }

            process(task);
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"VO_VOLATILE_INCREMENT" })
    private void process(Object task) {
        processedCount++;

        if (task instanceof Operation) {
            processOperation((Operation) task);
            return;
        }

        if (task instanceof Packet) {
            processPacket((Packet) task);
            return;
        }

        if (task instanceof PartitionSpecificRunnable) {
            processPartitionSpecificRunnable((PartitionSpecificRunnable) task);
            return;
        }

        throw new IllegalStateException("Unhandled task type for task:" + task);
    }

    private void processPartitionSpecificRunnable(PartitionSpecificRunnable runnable) {
        currentOperationHandler = getOperationHandler(runnable.getPartitionId());
        try {
            currentOperationHandler.process(runnable);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process task: " + runnable + " on " + getName());
        } finally {
            currentOperationHandler = null;
        }
    }

    private void processPacket(Packet packet) {
        currentOperationHandler = getOperationHandler(packet.getPartitionId());
        try {
            currentOperationHandler.process(packet);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process packet: " + packet + " on " + getName(), e);
        } finally {
            currentOperationHandler = null;
        }
    }

    private void processOperation(Operation operation) {
        currentOperationHandler = getOperationHandler(operation.getPartitionId());
        try {
            currentOperationHandler.process(operation);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process operation: " + operation + " on " + getName(), e);
        } finally {
            currentOperationHandler = null;
        }
    }

    public final void shutdown() {
        shutdown = true;
        interrupt();
    }

    public final void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
        long timeoutMs = unit.toMillis(timeout);
        join(timeoutMs);
    }
}
