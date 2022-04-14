package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.logging.ILogger;

public class ReactorConfig {
    public String name;
    public ThreadAffinity threadAffinity;
    public ILogger logger;
    public Scheduler scheduler;
}
