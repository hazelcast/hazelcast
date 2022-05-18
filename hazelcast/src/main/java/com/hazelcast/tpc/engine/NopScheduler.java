package com.hazelcast.tpc.engine;

import com.hazelcast.tpc.engine.frame.Frame;

public class NopScheduler implements Scheduler {
    @Override
    public void setEventloop(Eventloop eventloop) {
    }

    @Override
    public boolean tick() {
        return false;
    }

    @Override
    public void schedule(Frame task) {
    }
}
