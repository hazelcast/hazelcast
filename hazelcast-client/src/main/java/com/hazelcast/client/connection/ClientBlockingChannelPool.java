package com.hazelcast.client.connection;

public interface ClientBlockingChannelPool {

    ClientBlockingChannel get(int partitionId);

    void release(int partitionId, ClientBlockingChannel channel, Throwable t);
}
