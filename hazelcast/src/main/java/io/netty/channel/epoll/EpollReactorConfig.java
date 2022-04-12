package io.netty.channel.epoll;

import com.hazelcast.spi.impl.reactor.ReactorConfig;

public class EpollReactorConfig extends ReactorConfig {
    public boolean spin;
    public boolean writeThrough;
}
