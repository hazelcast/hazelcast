package com.hazelcast.spi.impl.engine;

import com.hazelcast.spi.impl.engine.frame.Frame;

public interface Scheduler {

    boolean tick();

    void schedule(Frame task);
}
