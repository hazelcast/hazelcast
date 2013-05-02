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

package com.hazelcast.clientv2;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.TcpIpConnection;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.SocketAddress;

public class ClientEndpoint implements Client {

    final Connection conn;
    final String uuid;
    LoginContext loginContext = null;
    volatile boolean authenticated = false;

    ClientEndpoint(Connection conn, String uuid) {
        this.conn = conn;
        this.uuid = uuid;
    }

    public void setLoginContext(LoginContext loginContext) {
        this.loginContext = loginContext;
    }

    public LoginContext getLoginContext() {
        return loginContext;
    }

    public Subject getSubject() {
        return loginContext != null ? loginContext.getSubject() : null;
    }

    public void authenticated() {
        this.authenticated = true;
    }

    public boolean isAuthenticated() {
        return authenticated;
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

    public void destroy() throws LoginException {
        final LoginContext lc = loginContext;
        if (lc != null) {
            lc.logout();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientEndpoint{");
        sb.append("conn=").append(conn);
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", authenticated=").append(authenticated);
        sb.append('}');
        return sb.toString();
    }
}
