package com.hazelcast.spi.impl.reactor;

import com.hazelcast.spi.impl.reactor.frame.Frame;

public interface Scheduler {

    boolean tick();

    void schedule(Frame task);
}
