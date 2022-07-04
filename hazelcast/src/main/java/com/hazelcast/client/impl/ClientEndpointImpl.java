/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.statistics.ClientStatistics;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.xa.XATransactionContextImpl;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;

/**
 * The {@link com.hazelcast.client.impl.ClientEndpoint} and {@link Client} implementation.
 */
public final class ClientEndpointImpl implements ClientEndpoint {
    private static final String METRICS_TAG_CLIENT = "client";
    private static final String METRICS_TAG_TIMESTAMP = "timestamp";
    private static final String METRICS_TAG_CLIENTNAME = "clientname";

    private final ClientEngine clientEngine;
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final ServerConnection connection;
    private final ConcurrentMap<UUID, TransactionContext> transactionContextMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, Callable> removeListenerActions = new ConcurrentHashMap<>();
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
    private volatile boolean destroyed;

    public ClientEndpointImpl(ClientEngine clientEngine, NodeEngineImpl nodeEngine, ServerConnection connection) {
        this.clientEngine = clientEngine;
        this.logger = clientEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.connection = connection;
        this.socketAddress = connection.getRemoteSocketAddress();
        this.clientVersion = "Unknown";
        this.creationTime = System.currentTimeMillis();
    }

    @Override
    public ServerConnection getConnection() {
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
        clientEngine.onEndpointAuthenticated(this);
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public String getClientVersion() {
        return clientVersion;
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
    public ClientStatistics getClientStatistics() {
        return statsRef.get();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) socketAddress;
    }

    @Override
    public String getClientType() {
        return connection.getConnectionType();
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
        if (destroyed) {
            removedAndRollbackTransactionContext(transactionContext.getTxnId());
        }
    }

    @Override
    public void removeTransactionContext(UUID txnId) {
        transactionContextMap.remove(txnId);
    }

    @Override
    public void addListenerDestroyAction(final String service, final String topic, final UUID id) {
        final EventService eventService = clientEngine.getEventService();
        addDestroyAction(id, () -> eventService.deregisterListener(service, topic, id));
    }

    @Override
    public void addDestroyAction(UUID registrationId, Callable<Boolean> removeAction) {
        removeListenerActions.put(registrationId, removeAction);
        if (destroyed) {
            removeAndCallRemoveAction(registrationId);
        }
    }

    @Override
    public boolean removeDestroyAction(UUID id) {
        return removeListenerActions.remove(id) != null;
    }

    public void destroy() throws LoginException {
        destroyed = true;
        nodeEngine.onClientDisconnected(getUuid());

        for (UUID registrationId : removeListenerActions.keySet()) {
            removeAndCallRemoveAction(registrationId);
        }

        for (UUID txnId : transactionContextMap.keySet()) {
            removedAndRollbackTransactionContext(txnId);
        }

        try {
            LoginContext lc = loginContext;
            if (lc != null) {
                lc.logout();
            }
        } finally {
            clientEngine.onEndpointDestroyed(this);
            authenticated = false;
        }
    }

    private void removeAndCallRemoveAction(UUID uuid) {
        Callable callable = removeListenerActions.remove(uuid);
        if (callable != null) {
            try {
                callable.call();
            } catch (Exception e) {
                logger.warning("Exception during remove listener action", e);
            }
        }
    }

    private void removedAndRollbackTransactionContext(UUID txnId) {
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
                + ", clientUuid=" + clientUuid
                + ", clientName=" + clientName
                + ", authenticated=" + authenticated
                + ", clientVersion=" + clientVersion
                + ", creationTime=" + creationTime
                + ", latest clientAttributes=" + getClientAttributes()
                + ", labels=" + labels
                + '}';
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        ClientStatistics clientStatistics = statsRef.get();
        if (clientStatistics != null && clientStatistics.metricsBlob() != null) {
            byte[] metricsBlob = clientStatistics.metricsBlob();
            if (metricsBlob.length == 0) {
                // zero length means that the client does not support the new format
                return;
            }
            long timestamp = clientStatistics.timestamp();
            MetricConsumer consumer = new MetricConsumer() {
                @Override
                public void consumeLong(MetricDescriptor descriptor, long value) {
                    context.collect(enhanceDescriptor(descriptor, timestamp), value);
                }

                @Override
                public void consumeDouble(MetricDescriptor descriptor, double value) {
                    context.collect(enhanceDescriptor(descriptor, timestamp), value);
                }

                private MetricDescriptor enhanceDescriptor(MetricDescriptor descriptor, long timestamp) {
                    return descriptor
                            // we exclude all metric targets here besides MANAGEMENT_CENTER
                            // since we want to send the client-side metrics only to MC
                            .withExcludedTargets(MetricTarget.ALL_TARGETS)
                            .withIncludedTarget(MANAGEMENT_CENTER)
                            // we add "client", "clientname" and "timestamp" tags for MC
                            .withTag(METRICS_TAG_CLIENT, getUuid().toString())
                            .withTag(METRICS_TAG_CLIENTNAME, clientName)
                            .withTag(METRICS_TAG_TIMESTAMP, Long.toString(timestamp));
                }
            };
            MetricsCompressor.extractMetrics(metricsBlob, consumer);
        }
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }
}
