package com.hazelcast.spi.impl.operationexecutor.impl;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import com.hazelcast.spi.Operation;


/**
 * There is work for different partitions. We just register this to all of them.
 * From the threadId we can infer the tasks that should run on the executor that
 * called {@link #next(int)} so we feed it the list.
 */
public final class BatchOperation implements Runnable {

    private final ConcurrentMap<Integer, Queue<Object>> tasksByThreadId;

    public static BatchOperation of(int partitionThreadCount, Operation... ops) {
        ConcurrentMap<Integer, Queue<Object>> tasksByThreadId = new ConcurrentHashMap<Integer, Queue<Object>>();
        for (Operation op : ops) {
            int threadId = getPartitionThreadId(op.getPartitionId(), partitionThreadCount);
            Queue<Object> queue = tasksByThreadId.get(threadId);
            if (queue == null) {
                queue = new ConcurrentLinkedQueue<Object>();
                tasksByThreadId.put(threadId, queue);
            }
            queue.add(op);
        }
        return new BatchOperation(tasksByThreadId);
    }

    public BatchOperation(ConcurrentMap<Integer, Queue<Object>> tasksByThreadId) {
        this.tasksByThreadId = tasksByThreadId;
    }

    public Object next(int threadId) {
        Queue<Object> queue = tasksByThreadId.get(threadId);
        return queue == null ? null : queue.poll();
    }

    private static int getPartitionThreadId(int partitionId, int partitionThreadCount) {
        return partitionId % partitionThreadCount;
    }

    @Override
    public void run() {
        // NOOP - will never be called, was just needed to get the batch executed
    }
}
