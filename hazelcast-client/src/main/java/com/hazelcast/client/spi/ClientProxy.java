package com.hazelcast.client.spi;

import com.hazelcast.core.DistributedObject;

/**
 * @mdogan 5/16/13
 */
public abstract class ClientProxy implements DistributedObject {

    private final String serviceName;

    private final Object objectId;

    private volatile ClientClusterService clusterService;

    private volatile ClientPartitionService partitionService;

    private volatile ClientInvocationService invocationService;

    protected ClientProxy(String serviceName, Object objectId) {
        this.serviceName = serviceName;
        this.objectId = objectId;
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

    public final ClientClusterService getClusterService() {
        return clusterService;
    }

    public final ClientPartitionService getPartitionService() {
        return partitionService;
    }

    public final ClientInvocationService getInvocationService() {
        return invocationService;
    }

    public final Object getId() {
        return objectId;
    }

    public final String getServiceName() {
        return serviceName;
    }

    public final void destroy() {
        onDestroy();
        clusterService = null;
        partitionService = null;
        invocationService = null;
    }

    protected abstract void onDestroy();
}
