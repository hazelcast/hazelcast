package com.hazelcast.client.spi;

import com.hazelcast.cluster.MulticastListener;

public interface ClientMulticastService {
    void addMulticastListener(MulticastListener multicastListener);
    void removeMulticastListener(MulticastListener multicastListener);

    void send(Object message);
}
