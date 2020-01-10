/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.xa.XATransactionContextImpl;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@link com.hazelcast.client.impl.ClientEndpoint} and {@link com.hazelcast.core.Client} implementation.
 */
public final class ClientEndpointImpl implements ClientEndpoint {

    private final ClientEngine clientEngine;
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final Connection connection;
    private final ConcurrentMap<String, TransactionContext> transactionContextMap
            = new ConcurrentHashMap<String, TransactionContext>();
    private final ConcurrentMap<String, Callable> removeListenerActions = new ConcurrentHashMap<String, Callable>();
    private final SocketAddress socketAddress;
    private final long creationTime;

    private LoginContext loginContext;
    private ClientPrincipal principal;
    private boolean ownerConnection;
    private Credentials credentials;
    private volatile boolean authenticated;
    private int clientVersion;
    private String clientVersionString;
    private long authenticationCorrelationId;
    private volatile String stats;
    private String clientName;
    private Set<String> labels;
    private volatile boolean destroyed;

    public ClientEndpointImpl(ClientEngine clientEngine, NodeEngineImpl nodeEngine, Connection connection) {
        this.clientEngine = clientEngine;
        this.logger = clientEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.connection = connection;
        if (connection instanceof TcpIpConnection) {
            TcpIpConnection tcpIpConnection = (TcpIpConnection) connection;
            socketAddress = tcpIpConnection.getRemoteSocketAddress();
        } else {
            socketAddress = null;
        }
        this.clientVersion = BuildInfo.UNKNOWN_HAZELCAST_VERSION;
        this.clientVersionString = "Unknown";
        this.creationTime = System.currentTimeMillis();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String getUuid() {
        return principal != null ? principal.getUuid() : null;
    }

    @Override
    public boolean isAlive() {
        return connection.isAlive();
    }

    @Override
    public void setLoginContext(LoginContext loginContext) {
        this.loginContext = loginContext;
    }

    @Override
    public Subject getSubject() {
        return loginContext != null ? loginContext.getSubject() : null;
    }

    public boolean isOwnerConnection() {
        return ownerConnection;
    }

    @Override
    public void authenticated(ClientPrincipal principal, Credentials credentials, boolean firstConnection, String clientVersion,
                              long authCorrelationId, String clientName, Set<String> labels) {
        this.principal = principal;
        this.ownerConnection = firstConnection;
        this.credentials = credentials;
        this.authenticated = true;
        this.authenticationCorrelationId = authCorrelationId;
        this.setClientVersion(clientVersion);
        this.clientName = clientName;
        this.labels = labels;
    }

    @Override
    public void authenticated(ClientPrincipal principal) {
        this.principal = principal;
        this.authenticated = true;
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public int getClientVersion() {
        return clientVersion;
    }

    @Override
    public void setClientVersion(String version) {
        clientVersionString = version;
        clientVersion = BuildInfo.calculateVersion(version);
    }

    @Override
    public void setClientStatistics(String stats) {
        this.stats = stats;
    }

    @Override
    public String getClientStatistics() {
        return stats;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) socketAddress;
    }

    @Override
    public ClientType getClientType() {
        ClientType type;
        switch (connection.getType()) {
            case JAVA_CLIENT:
                type = ClientType.JAVA;
                break;
            case CSHARP_CLIENT:
                type = ClientType.CSHARP;
                break;
            case CPP_CLIENT:
                type = ClientType.CPP;
                break;
            case PYTHON_CLIENT:
                type = ClientType.PYTHON;
                break;
            case RUBY_CLIENT:
                type = ClientType.RUBY;
                break;
            case NODEJS_CLIENT:
                type = ClientType.NODEJS;
                break;
            case GO_CLIENT:
                type = ClientType.GO;
                break;
            case BINARY_CLIENT:
                type = ClientType.OTHER;
                break;
            default:
                throw new IllegalArgumentException("Invalid connection type: " + connection.getType());
        }
        return type;
    }

    @Override
    public String getName() {
        return clientName;
    }

    @Override
    public Set<String> getLabels() {
        return labels;
    }

    @Override
    public TransactionContext getTransactionContext(String txnId) {
        final TransactionContext transactionContext = transactionContextMap.get(txnId);
        if (transactionContext == null) {
            throw new TransactionException("No transaction context found for txnId:" + txnId);
        }
        return transactionContext;
    }

    @Override
    public Credentials getCredentials() {
        return credentials;
    }

    @Override
    public void setTransactionContext(TransactionContext transactionContext) {
        transactionContextMap.put(transactionContext.getTxnId(), transactionContext);
        if (destroyed) {
            removedAndRollbackTransactionContext(transactionContext.getTxnId());
        }
    }

    @Override
    public void removeTransactionContext(String txnId) {
        transactionContextMap.remove(txnId);
    }

    @Override
    public void addListenerDestroyAction(final String service, final String topic, final String id) {
        final EventService eventService = clientEngine.getEventService();
        addDestroyAction(id, new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return eventService.deregisterListener(service, topic, id);
            }
        });
    }

    @Override
    public void addDestroyAction(String registrationId, Callable<Boolean> removeAction) {
        removeListenerActions.put(registrationId, removeAction);
        if (destroyed) {
            removeAndCallRemoveAction(registrationId);
        }
    }

    @Override
    public boolean removeDestroyAction(String id) {
        return removeListenerActions.remove(id) != null;
    }

    @Override
    public void clearAllListeners() {
        for (String registrationId : removeListenerActions.keySet()) {
            removeAndCallRemoveAction(registrationId);
        }
    }

    public void destroy() throws LoginException {
        destroyed = true;
        nodeEngine.onClientDisconnected(getUuid());

        LoginContext lc = loginContext;
        if (lc != null) {
            lc.logout();
        }

        clearAllListeners();

        for (String txnId : transactionContextMap.keySet()) {
            removedAndRollbackTransactionContext(txnId);
        }
        authenticated = false;
    }

    private void removeAndCallRemoveAction(String uuid) {
        Callable callable = removeListenerActions.remove(uuid);
        if (callable != null) {
            try {
                callable.call();
            } catch (Exception e) {
                logger.warning("Exception during remove listener action", e);
            }
        }
    }

    private void removedAndRollbackTransactionContext(String txnId) {
        TransactionContext context = transactionContextMap.remove(txnId);
        if (context != null) {
            if (context instanceof XATransactionContextImpl) {
                return;
            }
            try {
                context.rollbackTransaction();
            } catch (HazelcastInstanceNotActiveException e) {
                logger.finest(e);
            } catch (Exception e) {
                logger.warning(e);
            }
        }
    }


    @Override
    public String toString() {
        return "ClientEndpoint{"
                + "connection=" + connection
                + ", principal='" + principal
                + ", ownerConnection=" + ownerConnection
                + ", authenticated=" + authenticated
                + ", clientVersion=" + clientVersionString
                + ", creationTime=" + creationTime
                + ", latest statistics=" + stats
                + '}';
    }

    public long getAuthenticationCorrelationId() {
        return authenticationCorrelationId;
    }
}
