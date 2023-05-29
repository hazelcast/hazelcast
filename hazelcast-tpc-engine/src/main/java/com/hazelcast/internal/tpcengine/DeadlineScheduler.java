package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.BoundPriorityQueue;

import java.util.PriorityQueue;

public class DeadlineScheduler {
    private final PriorityQueue<DeadlineTask> runQueue;
    protected long earliestDeadlineNanos = -1;

    public DeadlineScheduler(int capacity){
        this.runQueue = new BoundPriorityQueue<>(capacity);
    }

    public long earliestDeadlineNanos(){
        return earliestDeadlineNanos;
    }

    public boolean offer(DeadlineTask task){
        return runQueue.offer(task);
    }

    public void tick(long nowNanos) {
        while (true) {
            DeadlineTask deadlineTask = runQueue.peek();

            if (deadlineTask == null) {
                return;
            }

            if (deadlineTask.deadlineNanos > nowNanos) {
                // Task should not yet be executed.
                earliestDeadlineNanos = deadlineTask.deadlineNanos;
                // we are done since all other tasks have a larger deadline.
                return;
            }

            // the deadlineTask first needs to be removed from the deadlineTask queue.
            runQueue.poll();
            earliestDeadlineNanos = -1;

            // offer the ScheduledTask to the task queue.
            deadlineTask.schedGroup.offer(deadlineTask);
        }
    }
}
