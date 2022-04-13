package com.hazelcast.spi.impl.reactor;

import io.netty.util.NetUtil;

public class SocketConfig {

    public int receiveBufferSize = 256 * 1024;
    public int sendBufferSize = 256 * 1024;
    public boolean tcpNoDelay = true;
    public boolean tcpQuickAck = true;
    public int backlog = NetUtil.SOMAXCONN;
}
