package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.nio.Address;

/**
 * @mdogan 5/16/13
 */
public final class ClientInvocationServiceImpl implements ClientInvocationService {

    private final HazelcastClient client;

    public ClientInvocationServiceImpl(HazelcastClient client) {
        this.client = client;
    }

    public Object invokeOnRandomTarget(Object request) {
        ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        return null;
    }

    public Object invokeOnTarget(Address target, Object request) {
        ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        return null;
    }

    public Object invokeOnKeyOwner(Object request, Object key) {
        ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();


        return null;
    }
}
