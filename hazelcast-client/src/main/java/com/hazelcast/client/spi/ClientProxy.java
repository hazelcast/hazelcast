package com.hazelcast.client.spi;

import com.hazelcast.spi.ObjectNamespace;

/**
 * @mdogan 5/16/13
 */
public abstract class ClientProxy {

    private final ObjectNamespace namespace;

    private volatile ClientClusterService clusterService;

    private volatile ClientPartitionService partitionService;

    private volatile ClientInvocationService invocationService;

    protected ClientProxy(ObjectNamespace namespace) {
        this.namespace = namespace;
    }

    final void setClusterService(ClientClusterService clusterService) {
        this.clusterService = clusterService;
    }

    final void setPartitionService(ClientPartitionService partitionService) {
        this.partitionService = partitionService;
    }

    final void setInvocationService(ClientInvocationService invocationService) {
        this.invocationService = invocationService;
    }

    public ObjectNamespace getNamespace() {
        return namespace;
    }

    public final ClientClusterService getClusterService() {
        return clusterService;
    }

    public final ClientPartitionService getPartitionService() {
        return partitionService;
    }

    public final ClientInvocationService getInvocationService() {
        return invocationService;
    }
}
