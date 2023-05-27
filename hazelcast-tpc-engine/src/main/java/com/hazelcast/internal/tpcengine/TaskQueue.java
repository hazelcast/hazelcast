package com.hazelcast.internal.tpcengine;

import java.util.Queue;

public class TaskQueue {

    public final String name;
    public final int shares;
    public final Queue<Object> queue;

    public TaskQueue(String name, int shares, Queue queue) {
        this.name = name;
        this.shares = shares;
        this.queue = queue;
    }
}
