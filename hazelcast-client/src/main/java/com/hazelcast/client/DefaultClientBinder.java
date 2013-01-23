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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

public class DefaultClientBinder implements ClientBinder {

    private final ILogger logger = Logger.getLogger(getClass().getName());
    ProtocolReader reader = new ProtocolReader();
    ProtocolWriter writer = new ProtocolWriter();

    public void bind(Connection connection, Credentials credentials) throws IOException {
        logger.log(Level.FINEST, connection + " -> "
                + connection.getAddress().getHostName() + ":" + connection.getSocket().getLocalPort());
        authProtocol(connection, credentials);
    }

    void write(Connection connection, Protocol command) throws IOException {
        command.onEnqueue();
        writer.write(connection, command);
        writer.flush(connection);
    }

    void authProtocol(Connection connection, Credentials credentials) throws IOException {
        String[] args = null;
        ByteBuffer[] bb = null;
        if (credentials instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials upCredentials = (UsernamePasswordCredentials) credentials;
            args = new String[]{upCredentials.getUsername(), upCredentials.getPassword()};
        } else {
//            bb = new ByteBuffer[]{ByteBuffer.wrap(IOUtil.toByteArray(credentials))};
        }
        Protocol auth = new Protocol(null, Command.AUTH, args, null);
        Protocol response = writeAndRead(connection, auth);
        logger.log(Level.FINEST, "auth response:" + response.command);
        if (!response.command.equals(Command.OK)) {
            throw new AuthenticationException("Client [" + connection + "] has failed authentication.");
        }
    }

    Protocol writeAndRead(Connection connection, Protocol command) throws IOException {
        write(connection, command);
        return reader.read(connection);
    }
}
