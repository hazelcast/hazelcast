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
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.SocketAddress;

public class ClientEndpoint implements Client {

    private final Connection conn;
    private String uuid;
    private LoginContext loginContext = null;
    private ClientPrincipal principal;
    private TransactionContext transactionContext;
    private boolean firstConnection = false;
    private volatile boolean authenticated = false;

    ClientEndpoint(Connection conn, String uuid) {
        this.conn = conn;
        this.uuid = uuid;
    }

    Connection getConnection() {
        return conn;
    }

    public String getUuid() {
        return uuid;
    }

    public boolean live() {
        return conn.live();
    }

    void setLoginContext(LoginContext loginContext) {
        this.loginContext = loginContext;
    }

    Subject getSubject() {
        return loginContext != null ? loginContext.getSubject() : null;
    }

    public boolean isFirstConnection() {
        return firstConnection;
    }

    void authenticated(ClientPrincipal principal, boolean firstConnection) {
        this.principal = principal;
        this.uuid = principal.getUuid();
        this.firstConnection = firstConnection;
        authenticated = true;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    public SocketAddress getSocketAddress() {
        if (conn instanceof TcpIpConnection) {
            ((TcpIpConnection) conn).getSocketChannelWrapper().socket().getRemoteSocketAddress();
        }
        return null;
    }

    public ClientType getClientType() {
        return ClientType.Native;
    }

    public TransactionContext getTransactionContext() {
        if (transactionContext == null){
            throw new TransactionException("No transaction context!!!");
        }
        return transactionContext;
    }

    public void setTransactionContext(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    void destroy() throws LoginException {
        final LoginContext lc = loginContext;
        if (lc != null) {
            lc.logout();
        }
        final TransactionContext context = transactionContext;
        if (context != null){
            try {
                context.rollbackTransaction();
            } catch (Exception e){
                e.printStackTrace();
            }
            transactionContext = null;
        }
        authenticated = false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientEndpoint{");
        sb.append("conn=").append(conn);
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", firstConnection=").append(firstConnection);
        sb.append(", authenticated=").append(authenticated);
        sb.append('}');
        return sb.toString();
    }
}
