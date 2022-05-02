package com.hazelcast.tpc.engine;

import com.hazelcast.tpc.engine.frame.Frame;

public interface Scheduler {

    void setEventloop(Eventloop eventloop);

    boolean tick();

    void schedule(Frame task);
}
