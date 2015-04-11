/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationParameters;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.logging.Level;

/**
 * Default Authentication with username password handling task
 */
public class AuthenticationMessageTask
        extends AuthenticationBaseMessageTask<AuthenticationParameters> {

    public AuthenticationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected AuthenticationParameters decodeClientMessage(ClientMessage clientMessage) {
        final AuthenticationParameters parameters = AuthenticationParameters.decode(clientMessage);
        final String uuid = parameters.uuid;
        final String ownerUuid = parameters.ownerUuid;
        if (uuid != null && uuid.length() > 0) {
            principal = new ClientPrincipal(uuid, ownerUuid);
        }
        credentials = new UsernamePasswordCredentials(parameters.username, parameters.password);
        return parameters;
    }

    protected boolean authenticate() {
        Connection connection = endpoint.getConnection();
        boolean authenticated = (clientEngine.getSecurityContext() == null) && authenticateWithUserNameAndPassword();
        logger.log((authenticated ? Level.INFO : Level.WARNING), "Received auth from " + connection + ", "
                + (authenticated ? "successfully authenticated" : "authentication failed"));
        return authenticated;
    }

    @Override
    protected boolean isOwnerConnection() {
        return parameters.isOwnerConnection;
    }

    private boolean authenticateWithUserNameAndPassword() {
        GroupConfig groupConfig = nodeEngine.getConfig().getGroupConfig();
        String nodeGroupName = groupConfig.getName();
        String nodeGroupPassword = groupConfig.getPassword();
        boolean usernameMatch = nodeGroupName.equals(parameters.username);
        boolean passwordMatch = nodeGroupPassword.equals(parameters.password);
        return usernameMatch && passwordMatch;
    }

}
