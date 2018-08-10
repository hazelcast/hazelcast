/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.client.impl.ClientStatsUtil.splitKeyValuePair;
import static com.hazelcast.client.impl.ClientStatsUtil.splitStats;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.xa.XATransactionContextImpl;

/**
 * The {@link com.hazelcast.client.impl.ClientEndpoint} and {@link com.hazelcast.core.Client} implementation.
 */
public final class ClientEndpointImpl implements ClientEndpoint, StatisticsAwareService<Object> {

    private final ClientEngineImpl clientEngine;
    private final NodeEngineImpl nodeEngine;
    private final Connection connection;
    private final ConcurrentMap<String, TransactionContext> transactionContextMap
            = new ConcurrentHashMap<String, TransactionContext>();
    private final ConcurrentHashMap<String, Callable> removeListenerActions = new ConcurrentHashMap<String, Callable>();
    private final SocketAddress socketAddress;
    
	@Probe
    private final long creationTime;

    private LoginContext loginContext;
    private ClientPrincipal principal;
    @Probe
    private boolean ownerConnection;
    private Credentials credentials;
    private volatile boolean authenticated;
    private int clientVersion;
    private String clientVersionString = "?";
    private long authenticationCorrelationId;
    
    private volatile String statsString;
    private final ClientStats stats = new ClientStats();
    private final ClientOSStats osStats = new ClientOSStats();
    private final ClientRuntimeStats runtimeStats = new ClientRuntimeStats();
    private final ConcurrentMap<String, ClientNearCacheStats> nearCacheStats = new ConcurrentHashMap<String, ClientNearCacheStats>();

    public ClientEndpointImpl(ClientEngineImpl clientEngine, NodeEngineImpl nodeEngine, Connection connection) {
        this.clientEngine = clientEngine;
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

	@Probe
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
    public void authenticated(ClientPrincipal principal, Credentials credentials, boolean firstConnection,
                              String clientVersion, long authCorrelationId) {
        this.principal = principal;
        this.ownerConnection = firstConnection;
        this.credentials = credentials;
        this.authenticated = true;
        this.authenticationCorrelationId = authCorrelationId;
        this.setClientVersion(clientVersion);
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
    
    private String getClientVersionString() {
		return clientVersionString;
	}

    @Override
    public void setClientVersion(String version) {
        clientVersionString = version;
        clientVersion = BuildInfo.calculateVersion(version);
    }
    
	public static String getEndpointPrefix(ClientEndpoint endpoint) {
		String type = endpoint.getClientType().name().toLowerCase();
		String address = endpoint.getSocketAddress().toString().replace("/", "");
		String version = endpoint instanceof ClientEndpointImpl ? ((ClientEndpointImpl) endpoint).getClientVersionString() : "" + endpoint.getClientVersion();
		return "client.endpoint[" + type + "][" + version + "][" + address + "][" + endpoint.getUuid() + "]";
	}

    @Override
    public void setClientStatistics(String stats) {
    	this.statsString = stats;
        Map<String, String> statMap = new HashMap<String, String>();
        if (stats != null) {
        	for (String keyValuePair : splitStats(stats)) {
        		List<String> keyAndValue = splitKeyValuePair(keyValuePair);
        		if (keyAndValue != null && keyAndValue.size() == 2) {
        			statMap.put(keyAndValue.get(0), keyAndValue.get(1));
        		}
        	}
        }
        this.stats.updateFrom(statMap);
        osStats.updateFrom(statMap);
        runtimeStats.updateFrom(statMap);
        Set<String> ncNames = ClientNearCacheStats.getNearCacheStatsDataStructureNames(statMap);
        nearCacheStats.keySet().retainAll(ncNames);
        for (String name : ncNames) {
        	if (nearCacheStats.containsKey(name)) {
        		nearCacheStats.get(name).updateFrom(statMap);
        	} else {
        		nearCacheStats.put(name, new ClientNearCacheStats(name));
        	}
        }
    }

    @Override
    public String getClientStatistics() {
        return statsString;
    }

    @Override
    public Map<String, Object> getStats() {
    	if (!ownerConnection) {
    		return Collections.emptyMap();
    	}
		Map<String, Object> res = new HashMap<String, Object>();
		String uuid = "[" + getUuid() + "]";
		res.put(uuid + "[" + stats.getName() + "]", stats);
		res.put("os" + uuid, osStats);
		res.put("runtime" + uuid, runtimeStats);
		for (ClientNearCacheStats ncStats : nearCacheStats.values()) {
			res.put(ncStats.getType() + "." + ncStats.getName() + ".nearcache" + uuid, ncStats);
		}
		return res;
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
    }

    @Override
    public boolean removeDestroyAction(String id) {
        return removeListenerActions.remove(id) != null;
    }

    @Override
    public void clearAllListeners() {
        for (Callable removeAction : removeListenerActions.values()) {
            try {
                removeAction.call();
            } catch (Exception e) {
                getLogger().warning("Exception during remove listener action", e);
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
                getLogger().finest(e);
            } catch (Exception e) {
                getLogger().warning(e);
            }
        }
        authenticated = false;
    }

    private ILogger getLogger() {
        return clientEngine.getLogger(getClass());
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
                + ", latest statistics=" + statsString
                + '}';
    }

    public long getAuthenticationCorrelationId() {
        return authenticationCorrelationId;
    }
}
