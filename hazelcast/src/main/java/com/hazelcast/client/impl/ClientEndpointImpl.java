/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.Client;
import com.hazelcast.client.ClientType;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.xa.XATransactionContextImpl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * The {@link com.hazelcast.client.impl.ClientEndpoint} and {@link Client} implementation.
 */
public final class ClientEndpointImpl implements ClientEndpoint {

    private final ClientEngine clientEngine;
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final Connection connection;
    private final ConcurrentMap<UUID, TransactionContext> transactionContextMap
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Callable> removeListenerActions = new ConcurrentHashMap<UUID, Callable>();
    private final SocketAddress socketAddress;
    private final long creationTime;

    private LoginContext loginContext;
    private UUID clientUuid;
    private Credentials credentials;
    private volatile boolean authenticated;
    private String clientVersion;
    private final AtomicReference<ClientStatistics> statsRef = new AtomicReference<>();
    private String clientName;
    private Set<String> labels;

    public ClientEndpointImpl(ClientEngine clientEngine, NodeEngineImpl nodeEngine, Connection connection) {
        this.clientEngine = clientEngine;
        this.logger = clientEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.connection = connection;
        this.socketAddress = connection.getRemoteSocketAddress();
        this.clientVersion = "Unknown";
        this.creationTime = System.currentTimeMillis();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public UUID getUuid() {
        return clientUuid;
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

    @Override
    public void authenticated(UUID clientUuid, Credentials credentials, String clientVersion,
                              long authCorrelationId, String clientName, Set<String> labels) {
        this.clientUuid = clientUuid;
        this.credentials = credentials;
        this.authenticated = true;
        this.setClientVersion(clientVersion);
        this.clientName = clientName;
        this.labels = labels;
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public void setClientVersion(String version) {
        clientVersion = version;
    }

    @Override
    public void setClientStatistics(ClientStatistics stats) {
        statsRef.set(stats);
    }

    @Override
    public String getClientAttributes() {
        ClientStatistics statistics = statsRef.get();
        return statistics != null ? statistics.clientAttributes() : null;
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
    public TransactionContext getTransactionContext(UUID txnId) {
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
    }

    @Override
    public void removeTransactionContext(UUID txnId) {
        transactionContextMap.remove(txnId);
    }

    @Override
    public void addListenerDestroyAction(final String service, final String topic, final UUID id) {
        final EventService eventService = clientEngine.getEventService();
        addDestroyAction(id, new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return eventService.deregisterListener(service, topic, id);
            }
        });
    }

    @Override
    public void addDestroyAction(UUID registrationId, Callable<Boolean> removeAction) {
        removeListenerActions.put(registrationId, removeAction);
    }

    @Override
    public boolean removeDestroyAction(UUID id) {
        return removeListenerActions.remove(id) != null;
    }

    @Override
    public void clearAllListeners() {
        for (Callable removeAction : removeListenerActions.values()) {
            try {
                removeAction.call();
            } catch (Exception e) {
                logger.warning("Exception during remove listener action", e);
            }
        }
        removeListenerActions.clear();
    }

    public void destroy() throws LoginException {
        clearAllListeners();
        nodeEngine.onClientDisconnected(getUuid());

        LoginContext lc = loginContext;
        if (lc != null) {
            lc.logout();
        }
        for (TransactionContext context : transactionContextMap.values()) {
            if (context instanceof XATransactionContextImpl) {
                continue;
            }
            try {
                context.rollbackTransaction();
            } catch (HazelcastInstanceNotActiveException e) {
                logger.finest(e);
            } catch (Exception e) {
                logger.warning(e);
            }
        }
        authenticated = false;
    }

    @Override
    public String toString() {
        return "ClientEndpoint{"
                + "connection=" + connection
                + ", clientUuid='" + clientUuid
                + ", authenticated=" + authenticated
                + ", clientVersion=" + clientVersion
                + ", creationTime=" + creationTime
                + ", latest clientAttributes=" + getClientAttributes()
                + '}';
    }
}
