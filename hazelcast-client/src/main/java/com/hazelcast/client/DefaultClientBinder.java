/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Bind;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.security.Credentials;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Level;

import static com.hazelcast.client.IOUtil.toByte;
import static com.hazelcast.client.IOUtil.toObject;

public class DefaultClientBinder implements ClientBinder {
    private HazelcastClient client;
    private final ILogger logger = Logger.getLogger(getClass().getName());

    public DefaultClientBinder(HazelcastClient client) {
        this.client = client;
    }

    public void bind(Connection connection, Credentials credentials) throws IOException {
        logger.log(Level.FINEST, connection + " -> "
                + connection.getAddress().getHostName() + ":" + connection.getSocket().getLocalPort());
        auth(connection, credentials);
//        Bind b = null;
//        try {
//            b = new Bind(new Address(connection.getAddress().getHostName(), connection.getSocket().getLocalPort()));
//        } catch (UnknownHostException e) {
//            logger.log(Level.WARNING, e.getMessage() + " while creating the bind package.");
//            throw e;
//        }
//        System.out.println("Client Bind = " + b);
//        Packet bind = new Packet();
//        bind.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, toByte(null), toByte(b));
//        write(connection, bind);
    }

    void auth(Connection connection, Credentials credentials) throws IOException {
        Packet auth = new Packet();
        auth.set("", ClusterOperation.CLIENT_AUTHENTICATE, new byte[0], toByte(credentials));
        Packet packet = writeAndRead(connection, auth);
        final Object response = toObject(packet.getValue());
        logger.log(Level.FINEST, "auth response:" + response);
        if (response instanceof Exception) {
            throw new RuntimeException((Exception) response);
        }
        if (!Boolean.TRUE.equals(response)) {
            throw new AuthenticationException("Client [" + connection + "] has failed authentication");
        }
    }

    Packet writeAndRead(Connection connection, Packet packet) throws IOException {
        write(connection, packet);
        return client.getInRunnable().reader.readPacket(connection);
    }

    void write(Connection connection, Packet packet) throws IOException {
        client.getOutRunnable().write(connection, packet);
    }
}
