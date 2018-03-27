package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Convenience for executing {@link Callable} on a partition thread.
 * Unlike regular {@link PartitionSpecificRunnable} it's easy to return a value back to a caller.
 *
 * This is intended to be used in tests only.
 *
 */
public final class TestTaskExecutorUtil {
    private static final Object NULL_VALUE = new Object();
    private static final int TIMEOUT_SECONDS = 120;

    private TestTaskExecutorUtil() {

    }

    /**
     * Executes a callable on a specific partition thread and return a result.
     * This does NOT check if a given Hazelcast instance owns a specific partition.
     *
     * @param instance Hazelcast instance to be used for task executin
     * @param task the task to be executed
     * @param partitionId selects partition thread
     * @param <T> type of the result
     * @return result as returned by the callable
     */
    public static <T> T runOnPartitionThread(HazelcastInstance instance, final Callable<T> task, final int partitionId) {
        InternalOperationService operationService = getNodeEngineImpl(instance).getOperationService();
        final BlockingQueue<Object> resultQueue = new ArrayBlockingQueue<Object>(1);
        operationService.execute(new PartitionSpecificRunnable() {
            @Override
            public int getPartitionId() {
                return partitionId;
            }

            @Override
            public void run() {
                try {
                    Object result = wrapNullIfNeeded(task.call());
                    resultQueue.add(result);
                } catch (Throwable e) {
                    resultQueue.add(e);
                }
            }
        });
        try {
            Object result = resultQueue.poll(TIMEOUT_SECONDS, SECONDS);
            if (result instanceof Throwable) {
                ExceptionUtil.sneakyThrow((Throwable) result);
            }
            return (T) unwrapNullIfNeeded(result);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IllegalStateException("Interrupted while waiting for result", e);
        }
    }

    private static Object wrapNullIfNeeded(Object object) {
        return object == null ? NULL_VALUE : object;
    }

    private static Object unwrapNullIfNeeded(Object object) {
        return object == NULL_VALUE ? null : object;
    }
}
