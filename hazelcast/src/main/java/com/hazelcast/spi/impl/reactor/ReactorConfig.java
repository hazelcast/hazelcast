package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;

public class ReactorConfig {
    public ReactorFrontEnd frontend;
    public ChannelConfig channelConfig;
    public Address thisAddress;
    public int port;
    public String name;
    public boolean poolRequests;
    public boolean poolResponses;
}
