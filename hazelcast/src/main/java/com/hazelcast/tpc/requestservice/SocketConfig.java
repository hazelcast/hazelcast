package com.hazelcast.tpc.requestservice;

import io.netty.util.NetUtil;

public class SocketConfig {

    public int receiveBufferSize = 256 * 1024;
    public int sendBufferSize = 256 * 1024;
    public boolean tcpNoDelay = true;
    public boolean tcpQuickAck = true;
    public int backlog = NetUtil.SOMAXCONN;
    public boolean keepAlive = true;
    public int soTimeout = 0;
}
