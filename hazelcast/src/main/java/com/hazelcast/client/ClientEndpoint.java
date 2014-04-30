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

package com.hazelcast.client;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionAccessor;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;

public final class ClientEndpoint implements Client {

    private final ClientEngineImpl clientEngine;
    private final Connection conn;
    private final ConcurrentMap<String, TransactionContext> transactionContextMap
            = new ConcurrentHashMap<String, TransactionContext>();
    private final List<Runnable> removeListenerActions = Collections.synchronizedList(new LinkedList<Runnable>());
    private final SocketAddress socketAddress;

    private String uuid;
    private LoginContext loginContext;
    private ClientPrincipal principal;
    private boolean firstConnection;
    private volatile boolean authenticated;

    ClientEndpoint(ClientEngineImpl clientEngine, Connection conn, String uuid) {
        this.clientEngine = clientEngine;
        this.conn = conn;
        if (conn instanceof TcpIpConnection) {
            TcpIpConnection tcpIpConnection = (TcpIpConnection) conn;
            socketAddress = tcpIpConnection.getSocketChannelWrapper().socket().getRemoteSocketAddress();
        } else {
            socketAddress = null;
        }
        this.uuid = uuid;
    }

    Connection getConnection() {
        return conn;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public boolean live() {
        return conn.live();
    }

    void setLoginContext(LoginContext loginContext) {
        this.loginContext = loginContext;
    }

    public Subject getSubject() {
        return loginContext != null ? loginContext.getSubject() : null;
    }

    public boolean isFirstConnection() {
        return firstConnection;
    }

    void authenticated(ClientPrincipal principal, boolean firstConnection) {
        this.principal = principal;
        this.uuid = principal.getUuid();
        this.firstConnection = firstConnection;
        this.authenticated = true;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) socketAddress;
    }

    @Override
    public ClientType getClientType() {
        switch (conn.getType()) {
            case JAVA_CLIENT:
                return ClientType.JAVA;
            case CSHARP_CLIENT:
                return ClientType.CSHARP;
            case CPP_CLIENT:
                return ClientType.CPP;
            case PYTHON_CLIENT:
                return ClientType.PYTHON;
            case RUBY_CLIENT:
                return ClientType.RUBY;
            case BINARY_CLIENT:
                return ClientType.OTHER;
            default:
                throw new IllegalArgumentException("Invalid connection type: " + conn.getType());
        }
    }

    public TransactionContext getTransactionContext(String txnId) {
        final TransactionContext transactionContext = transactionContextMap.get(txnId);
        if (transactionContext == null) {
            throw new TransactionException("No transaction context found for txnId:" + txnId);
        }
        return transactionContext;
    }

    public void setTransactionContext(TransactionContext transactionContext) {
        transactionContextMap.put(transactionContext.getTxnId(), transactionContext);
    }

    public void removeTransactionContext(String txnId) {
        transactionContextMap.remove(txnId);
    }

    public void setListenerRegistration(final String service, final String topic, final String id) {
        removeListenerActions.add(new Runnable() {
            @Override
            public void run() {
                EventService eventService = clientEngine.getEventService();
                eventService.deregisterListener(service, topic, id);
            }
        });
    }

    public void setDistributedObjectListener(final String id) {
        removeListenerActions.add(new Runnable() {
            @Override
            public void run() {
                clientEngine.getProxyService().removeProxyListener(id);
            }
        });
    }

    public void clearAllListeners() {
        for (Runnable removeAction : removeListenerActions) {
            try {
                removeAction.run();
            } catch (Exception e) {
                getLogger().warning("Exception during destroy action", e);
            }
        }
        removeListenerActions.clear();
    }

    void destroy() throws LoginException {
        for (Runnable removeAction : removeListenerActions) {
            try {
                removeAction.run();
            } catch (Exception e) {
                getLogger().warning("Exception during destroy action", e);
            }
        }

        LoginContext lc = loginContext;
        if (lc != null) {
            lc.logout();
        }
        for (TransactionContext context : transactionContextMap.values()) {
            Transaction transaction = TransactionAccessor.getTransaction(context);
            if (context.isXAManaged() && transaction.getState() == PREPARED) {
                TransactionManagerServiceImpl transactionManager =
                        (TransactionManagerServiceImpl) clientEngine.getTransactionManagerService();
                transactionManager.addTxBackupLogForClientRecovery(transaction);
            } else {
                try {
                    context.rollbackTransaction();
                } catch (HazelcastInstanceNotActiveException e) {
                    getLogger().finest(e);
                } catch (Exception e) {
                    getLogger().warning(e);
                }
            }
        }
        authenticated = false;
    }

    private ILogger getLogger() {
        return clientEngine.getLogger(getClass());
    }

    public void sendResponse(Object response, int callId) {
        boolean isError = false;
        Object clientResponseObject;
        if (response == null) {
            clientResponseObject = ClientEngineImpl.NULL;
        } else if (response instanceof Throwable) {
            isError = true;
            ClientExceptionConverter converter = ClientExceptionConverters.get(getClientType());
            clientResponseObject = converter.convert((Throwable) response);
        } else {
            clientResponseObject = response;
        }
        ClientResponse clientResponse = new ClientResponse(clientEngine.toData(clientResponseObject), isError, callId);
        clientEngine.sendResponse(this, clientResponse);
    }

    public void sendEvent(Object event, int callId) {
        Data data = clientEngine.toData(event);
        clientEngine.sendResponse(this, new ClientResponse(data, callId, true));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ClientEndpoint{");
        sb.append("conn=").append(conn);
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", firstConnection=").append(firstConnection);
        sb.append(", authenticated=").append(authenticated);
        sb.append('}');
        return sb.toString();
    }
}
