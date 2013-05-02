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

import com.hazelcast.config.GroupConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.logging.Level;

public class ClientAuthenticationRequest extends AbstractClientRequest implements ClientRequest {

    private Credentials credentials;

    public ClientAuthenticationRequest() {
    }

    public ClientAuthenticationRequest(Credentials credentials) {
        this.credentials = credentials;
    }

    public Object process() throws Exception {
        ClientEngineImpl clientService = (ClientEngineImpl) service;
        ILogger logger = clientEngine.getILogger(getClass());
        boolean authenticated;
        if (credentials == null) {
            authenticated = false;
            logger.log(Level.SEVERE, "Could not retrieve Credentials object!");
        } else if (clientService.getSecurityContext() != null) {
            credentials.setEndpoint(connection.getInetAddress().getHostAddress());
            try {
                SecurityContext securityContext = clientService.getSecurityContext();
                LoginContext lc = securityContext.createClientLoginContext(credentials);
                lc.login();
                clientService.getEndpoint(connection).setLoginContext(lc);
                authenticated = true;
            } catch (LoginException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
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
                logger.log(Level.SEVERE, "Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster " +
                        "group-name and group-password should be used for authentication!\n" +
                        "Current credentials type is: " + credentials.getClass().getName());
            }
        }
        logger.log((authenticated ? Level.INFO : Level.WARNING), "Received auth from " + connection
                + ", " + (authenticated ? "successfully authenticated" : "authentication failed"));
        if (!authenticated) {
            clientService.removeEndpoint(connection);
            return false;
        } else {
            ClientEndpoint clientEndpoint = clientService.getEndpoint(connection);
            clientEndpoint.authenticated();
            clientService.bind(connection);
            return true;
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

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writePortable("cred", (Portable) credentials);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        credentials = (Credentials) reader.readPortable("cred");

    }
}
