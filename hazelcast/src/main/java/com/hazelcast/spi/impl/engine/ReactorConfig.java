package com.hazelcast.spi.impl.engine;

import com.hazelcast.logging.ILogger;

public class ReactorConfig {
    public String name;
    public ILogger logger;
    public Scheduler scheduler;
}
