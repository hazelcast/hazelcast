package com.hazelcast.internal.tpcengine;

import java.util.ArrayList;
import java.util.PriorityQueue;

class CfsScheduler {

    private PriorityQueue<SchedulingGroup> priorityQueue = new PriorityQueue();

    private long min_vruntime;

    public long min_vruntime() {
        return min_vruntime;
    }

    public SchedulingGroup next(){
        return priorityQueue.poll();
    }

    public void insert(SchedulingGroup taskQueue) {
        priorityQueue.add(taskQueue);
    }

}
