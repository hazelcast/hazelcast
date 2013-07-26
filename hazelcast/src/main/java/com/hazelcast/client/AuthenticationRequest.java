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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Collection;
import java.util.logging.Level;

public final class AuthenticationRequest extends CallableClientRequest implements Portable {

    private Credentials credentials;

    private ClientPrincipal principal;

    private boolean reAuth;

    private boolean firstConnection = false;

    public AuthenticationRequest() {
    }

    public AuthenticationRequest(Credentials credentials) {
        this.credentials = credentials;
    }

    public AuthenticationRequest(Credentials credentials, ClientPrincipal principal) {
        this.credentials = credentials;
        this.principal = principal;
    }

    public Object call() throws Exception {
        ClientEngineImpl clientEngine = getService();
        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());
        clientEngine.sendResponse(endpoint, clientEngine.getThisAddress());
        boolean authenticated;
        if (credentials == null) {
            authenticated = false;
            logger.severe("Could not retrieve Credentials object!");
        } else if (clientEngine.getSecurityContext() != null) {
            credentials.setEndpoint(connection.getInetAddress().getHostAddress());
            try {
                SecurityContext securityContext = clientEngine.getSecurityContext();
                LoginContext lc = securityContext.createClientLoginContext(credentials);
                lc.login();
                endpoint.setLoginContext(lc);
                authenticated = true;
            } catch (LoginException e) {
                logger.warning(e);
                authenticated = false;
            }
        } else {
            if (credentials instanceof UsernamePasswordCredentials) {
                final UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
                GroupConfig groupConfig = clientEngine.getConfig().getGroupConfig();
                final String nodeGroupName = groupConfig.getName();
                final String nodeGroupPassword = groupConfig.getPassword();
                authenticated = (nodeGroupName.equals(usernamePasswordCredentials.getUsername())
                        && nodeGroupPassword.equals(usernamePasswordCredentials.getPassword()));
            } else {
                authenticated = false;
                logger.severe("Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster " +
                        "group-name and group-password should be used for authentication!\n" +
                        "Current credentials type is: " + credentials.getClass().getName());
            }
        }
        logger.log((authenticated ? Level.INFO : Level.WARNING), "Received auth from " + connection
                + ", " + (authenticated ? "successfully authenticated" : "authentication failed"));
        if (authenticated) {
            if (principal != null) {
                final ClusterService clusterService = clientEngine.getClusterService();
                if (reAuth) {
                    //TODO @ali check reAuth

//                    if (clusterService.getMember(principal.getOwnerUuid()) != null) {
//                        return new AuthenticationException("Owner member is already member of this cluster, cannot re-auth!");
//                    } else {
                        principal = new ClientPrincipal(principal.getUuid(), clientEngine.getLocalMember().getUuid());
                        final Collection<MemberImpl> members = clientEngine.getClusterService().getMemberList();
                        for (MemberImpl member : members) {
                            if (!member.localMember()) {
                                clientEngine.sendOperation(new ClientReAuthOperation(principal.getUuid(), firstConnection), member.getAddress());
                            }
                        }
//                    }
                }
//                else if (clusterService.getMember(principal.getOwnerUuid()) == null) {
//                    clientEngine.removeEndpoint(connection);
//                    return new AuthenticationException("Owner member is not member of this cluster!");
//                }
            }
            if (principal == null) {
                principal = new ClientPrincipal(endpoint.getUuid(), clientEngine.getLocalMember().getUuid());
            }
            endpoint.authenticated(principal, firstConnection);
            clientEngine.bind(endpoint);
            return principal;
        } else {
            clientEngine.removeEndpoint(connection);
            return new AuthenticationException("Invalid credentials!");
        }
    }

    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return 2;
    }

    public void setReAuth(boolean reAuth) {
        this.reAuth = reAuth;
    }

    public boolean isFirstConnection() {
        return firstConnection;
    }

    public void setFirstConnection(boolean firstConnection) {
        this.firstConnection = firstConnection;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writePortable("credentials", (Portable) credentials);
        if (principal != null) {
            writer.writePortable("principal", principal);
        } else {
            writer.writeNullPortable("principal", ClientPortableHook.ID, ClientPortableHook.PRINCIPAL);
        }
        writer.writeBoolean("reAuth", reAuth);
        writer.writeBoolean("firstConnection", firstConnection);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        credentials = (Credentials) reader.readPortable("credentials");
        principal = reader.readPortable("principal");
        reAuth = reader.readBoolean("reAuth");
        firstConnection = reader.readBoolean("firstConnection");
    }
}
