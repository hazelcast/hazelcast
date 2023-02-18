package com.hazelcast.internal.tpc;

public interface CrappyThread {
    void setEventloopTask(Runnable eventloopTask);
}
