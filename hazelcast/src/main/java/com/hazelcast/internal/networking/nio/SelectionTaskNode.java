package com.hazelcast.internal.networking.nio;

public class SelectionTaskNode {
    public SelectionTaskNode next;
    public Runnable task;
    public int length;
}
