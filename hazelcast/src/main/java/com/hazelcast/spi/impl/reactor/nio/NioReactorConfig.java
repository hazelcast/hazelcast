package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.spi.impl.reactor.ReactorConfig;

public class NioReactorConfig extends ReactorConfig {
    public boolean spin;
    public boolean writeThrough;
}
