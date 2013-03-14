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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

public class DefaultClientBinder {

    private final ILogger logger = Logger.getLogger(getClass().getName());
    private final SerializationService serializationService;
    private final ProtocolReader reader;
    private final ProtocolWriter writer;
    private final Credentials credentials;

    public DefaultClientBinder(SerializationService serializationService, Credentials credentials) {
        this.serializationService = serializationService;
        this.credentials = credentials;
        this.reader = new ProtocolReader(serializationService);
        this.writer = new ProtocolWriter(serializationService);
    }



    public void bind(Connection connection) throws IOException {
        logger.log(Level.FINEST, connection + " -> "+ connection.getAddress());
        authProtocol(connection, credentials);
    }

    void write(Connection connection, Protocol command) throws IOException {
        command.onEnqueue();
        writer.write(connection, command);
        writer.flush(connection);
    }

    void authProtocol(Connection connection, Credentials credentials) throws IOException {
        String[] args = null;
        Protocol auth;
        if (credentials instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials upCredentials = (UsernamePasswordCredentials) credentials;
            args = new String[]{upCredentials.getUsername(), upCredentials.getPassword()};
            auth = new Protocol(null, Command.AUTH, args, null);
        } else {
            Data cr = serializationService.toData(credentials);
            auth = new Protocol(null, Command.AUTH, new String[]{}, cr);
        }
        Protocol response = writeAndRead(connection, auth);
        logger.log(Level.FINEST, "auth response:" + response.command);
        if (!response.command.equals(Command.OK)) {
            throw new AuthenticationException("Client [" + connection + "] has failed authentication. Response was " + response.command);
        }
    }

    Protocol writeAndRead(Connection connection, Protocol command) throws IOException {
        write(connection, command);
        return reader.read(connection);
    }
}
