package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.util.concurrent.TimeUnit;

public class ClientDummyInvocationServiceImpl extends ClientInvocationServiceSupport {

    private final ClusterListenerSupport clusterListenerSupport;

    public ClientDummyInvocationServiceImpl(HazelcastClientInstanceImpl client) {
        super(client);
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        clusterListenerSupport = clusterService.getClusterListenerSupport();
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request) throws Exception {
        return sendToOwner(request, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        return sendToOwner(request, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key) throws Exception {
        return sendToOwner(request, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnConnection(ClientRequest request, ClientConnection connection) {
        return invokeOnConnection(request, connection, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnPartitionOwner(ClientRequest request, int partitionId) throws Exception {
        return sendToOwner(request, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request, EventHandler handler)
            throws Exception {
        return sendToOwner(request, handler);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnConnection(ClientRequest request, ClientConnection connection,
                                                        EventHandler handler) {
        if (connection == null) {
            throw new NullPointerException("Connection can not be null");
        }
        request.setSingleConnection();
        return send(request, connection, handler);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target, EventHandler handler)
            throws Exception {
        return sendToOwner(request, handler);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key, EventHandler handler)
            throws Exception {
        return sendToOwner(request, handler);
    }

    protected <T> ICompletableFuture<T> sendToOwner(ClientRequest request, EventHandler handler) throws Exception {

        int count = 0;
        Exception lastError = null;
        long retryCount = retryTimeoutInSeconds / RETRY_WAIT_TIME_IN_SECONDS;
        while (count++ < retryCount) {
            try {
                final Address ownerConnectionAddress = clusterListenerSupport.getOwnerConnectionAddress();
                if (ownerConnectionAddress != null) {
                    final Connection conn = connectionManager.getConnection(ownerConnectionAddress);
                    if (conn != null) {
                        return send(request, (ClientConnection) conn, handler);
                    }
                }
            } catch (HazelcastInstanceNotActiveException e) {
                lastError = e;
            }
            if (!client.getLifecycleService().isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_WAIT_TIME_IN_SECONDS));
        }
        if (lastError == null) {
            throw new OperationTimeoutException("Could not invoke request on cluster in "
                    + retryTimeoutInSeconds + " seconds");
        }
        throw lastError;
    }


    @Override
    public <T> ICompletableFuture<T> reSend(ClientCallFuture future) throws Exception {
        final Connection conn = connectionManager.getConnection(clusterListenerSupport.getOwnerConnectionAddress());
        sendInternal(future, (ClientConnection) conn);
        return future;
    }

}
