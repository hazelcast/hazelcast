package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.logging.ILogger;

public class ReactorConfig {
    public ReactorFrontEnd frontend;
    public String name;
    public boolean poolRequests;
    public boolean poolLocalResponses;
    public boolean poolRemoteResponses;
    public ThreadAffinity threadAffinity;
    public ILogger logger;
    public Managers managers;
}
