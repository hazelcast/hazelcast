package com.hazelcast.internal.tpcengine;

import java.util.ArrayList;
import java.util.PriorityQueue;

import static java.lang.Math.max;

class CfsScheduler {

    private PriorityQueue<SchedulingGroup> priorityQueue = new PriorityQueue();

    private long min_vruntime;

    public long min_vruntime() {
        return min_vruntime;
    }

    public SchedulingGroup next(){
        return priorityQueue.poll();
    }

    public void insert(SchedulingGroup schedGroup) {
        schedGroup.state = SchedulingGroup.STATE_RUNNING;
        schedGroup.vruntimeNanos = max(schedGroup.vruntimeNanos, min_vruntime());
        priorityQueue.add(schedGroup);
    }

}
