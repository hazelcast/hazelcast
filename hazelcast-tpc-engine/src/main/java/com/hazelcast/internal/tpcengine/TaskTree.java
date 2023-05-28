package com.hazelcast.internal.tpcengine;

import java.util.PriorityQueue;

class TaskTree {
    private PriorityQueue<TaskQueue> priorityQueue = new PriorityQueue();

    public TaskQueue next(){
        return priorityQueue.poll();
    }

    public void insert(TaskQueue taskQueue) {
        priorityQueue.add(taskQueue);
    }
}
