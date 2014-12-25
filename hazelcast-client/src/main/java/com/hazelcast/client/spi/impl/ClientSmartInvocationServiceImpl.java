/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class ClientSmartInvocationServiceImpl extends ClientInvocationServiceSupport {

    private final LoadBalancer loadBalancer;
    private final Credentials credentials;
    private final ClusterAuthenticator authenticator;

    public ClientSmartInvocationServiceImpl(HazelcastClientInstanceImpl client, LoadBalancer loadBalancer) {
        super(client);
        this.loadBalancer = loadBalancer;
        credentials = client.getCredentials();
        authenticator = new ClusterAuthenticator();
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request) throws Exception {
        return invokeOnRandomTarget(request, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        return invokeOnTarget(request, target, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key) throws Exception {
        return invokeOnKeyOwner(request, key, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnPartitionOwner(ClientRequest request, int partitionId) throws Exception {
        return invokeOnPartitionOwner(request, partitionId, null);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key, EventHandler handler)
            throws Exception {
        int partitionId = partitionService.getPartitionId(key);
        return invokeOnPartitionOwner(request, partitionId, handler);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnConnection(ClientRequest request, ClientConnection connection) {
        return invokeOnConnection(request, connection, null);
    }

    private <T> ICompletableFuture<T> invokeOnPartitionOwner(ClientRequest request, int partitionId
            , EventHandler handler) throws Exception {
        Exception lastError = null;
        int count = 0;
        long retryCount = retryTimeoutInSeconds / RETRY_WAIT_TIME_IN_SECONDS;
        while (count++ < retryCount) {
            try {
                final Address owner = partitionService.getPartitionOwner(partitionId);
                if (owner != null) {
                    request.setPartitionId(partitionId);
                    ClientConnection connection =
                            (ClientConnection) connectionManager.getOrConnect(owner, authenticator);
                    return send(request, connection, handler);
                }
            } catch (IOException e) {
                lastError = e;
            } catch (HazelcastInstanceNotActiveException e) {
                lastError = e;
            }
            if (!client.getLifecycleService().isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_WAIT_TIME_IN_SECONDS));
        }
        if (lastError == null) {
            throw new OperationTimeoutException("Could not invoke request on partition(" + partitionId + ") owner in "
                    + retryTimeoutInSeconds + " seconds");
        }
        throw lastError;
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request,
                                                          EventHandler handler) throws Exception {
        Exception lastError = null;
        int count = 0;
        long retryCount = retryTimeoutInSeconds / RETRY_WAIT_TIME_IN_SECONDS;
        while (count++ < retryCount) {
            try {
                final Address randomAddress = getRandomAddress();
                if (randomAddress != null) {
                    final Connection connection = connectionManager.getOrConnect(randomAddress, authenticator);
                    return send(request, (ClientConnection) connection, handler);
                }
            } catch (IOException e) {
                lastError = e;
            } catch (HazelcastInstanceNotActiveException e) {
                lastError = e;
            }
            if (!client.getLifecycleService().isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_WAIT_TIME_IN_SECONDS));

        }
        if (lastError == null) {
            throw new OperationTimeoutException("Could not invoke request on any target in "
                    + retryTimeoutInSeconds + " seconds");
        }
        throw lastError;
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target, EventHandler handler)
            throws Exception {
        if (target == null) {
            throw new NullPointerException("Target can not be null");
        }
        int count = 0;
        Exception lastError = null;
        long retryCount = retryTimeoutInSeconds / RETRY_WAIT_TIME_IN_SECONDS;
        while (count++ < retryCount) {
            try {
                if (isMember(target)) {
                    final Connection connection = connectionManager.getOrConnect(target, authenticator);
                    return invokeOnConnection(request, (ClientConnection) connection, handler);
                }
            } catch (IOException e) {
                lastError = e;
            } catch (HazelcastInstanceNotActiveException e) {
                lastError = e;
            }
            if (!client.getLifecycleService().isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_WAIT_TIME_IN_SECONDS));
        }
        if (lastError == null) {
            throw new OperationTimeoutException("Could not invoke request on the target(" + target + ") in "
                    + retryTimeoutInSeconds + " seconds");
        }
        throw lastError;
    }


    @Override
    public <T> ICompletableFuture<T> invokeOnConnection(ClientRequest request, ClientConnection connection,
                                                        EventHandler handler) {
        request.setSingleConnection();
        return send(request, connection, handler);
    }

    // NIO public
    public <T> ICompletableFuture<T> reSend(ClientCallFuture future) throws Exception {
        final Address randomAddress = getRandomAddress();
        if (randomAddress != null) {
            final Connection connection = connectionManager.getOrConnect(randomAddress, authenticator);
            sendInternal(future, (ClientConnection) connection);
            return future;
        }
        if (!client.getLifecycleService().isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        } else {
            throw new IllegalStateException("Noa address found to retry! ");
        }
    }


    private Address getRandomAddress() {
        MemberImpl member = (MemberImpl) loadBalancer.next();
        if (member != null) {
            return member.getAddress();
        }
        return null;
    }

    private boolean isMember(Address target) {
        final MemberImpl member = client.getClientClusterService().getMember(target);
        return member != null;
    }


    private class ClusterAuthenticator implements Authenticator {
        @Override
        public void authenticate(ClientConnection connection) throws AuthenticationException, IOException {
            final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
            final ClientPrincipal principal = clusterService.getClusterListenerSupport().getPrincipal();
            final SerializationService ss = client.getSerializationService();
            AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
            connection.init();
            //contains remoteAddress and principal
            SerializableCollection collectionWrapper;
            final Future future = client.getInvocationService().invokeOnConnection(auth, connection);
            try {
                collectionWrapper = ss.toObject(future.get());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e, IOException.class);
            }
            final Iterator<Data> iter = collectionWrapper.iterator();
            final Data addressData = iter.next();
            final Address address = ss.toObject(addressData);
            connection.setRemoteEndpoint(address);
        }
    }

}
