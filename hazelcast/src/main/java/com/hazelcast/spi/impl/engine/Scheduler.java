package com.hazelcast.spi.impl.engine;

import com.hazelcast.spi.impl.engine.frame.Frame;

public interface Scheduler {

    void setReactor(Reactor reactor);

    boolean tick();

    void schedule(Frame task);
}
