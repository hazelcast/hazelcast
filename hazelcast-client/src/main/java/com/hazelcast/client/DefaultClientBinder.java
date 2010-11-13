/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import static com.hazelcast.client.Serializer.toByte;
import static com.hazelcast.client.Serializer.toObject;

import com.hazelcast.client.cluster.Bind;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Level;

public class DefaultClientBinder implements ClientBinder {
    private HazelcastClient client;
    private final ILogger logger = Logger.getLogger(getClass().getName());

    public DefaultClientBinder(HazelcastClient client) {
        this.client = client;
    }

    public void bind(Connection connection) throws IOException {
        Bind b = null;
        try {
            b = new Bind(new Address(connection.getAddress().getHostName(), connection.getSocket().getLocalPort()));
        } catch (UnknownHostException e) {
            logger.log(Level.WARNING, e.getMessage() + " while creating the bind package.");
            throw e;
        }
        Packet bind = new Packet();
        bind.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, toByte(null), toByte(b));
        logger.log(Level.FINEST, "bind");
        write(connection, bind);
        logger.log(Level.FINEST, "bind responce");
        
        auth(connection);
    }

    void auth(Connection connection) throws IOException {
        Packet auth = new Packet();
        final GroupConfig groupConfig = client.groupConfig();
        auth.set("", ClusterOperation.CLIENT_AUTHENTICATE, 
            toByte(groupConfig.getName()), toByte(groupConfig.getPassword()));
        Packet packet = writeAndRead(connection, auth);
        final Object response = toObject(packet.getValue());
        logger.log(Level.FINEST, "auth responce:" + response);
        if (response instanceof Exception){
            throw new RuntimeException((Exception)response);
        }
        if (!Boolean.TRUE.equals(response)){
            throw new AuthenticationException("Client [" + connection + "] has failed authentication");
        }
    }

    Packet writeAndRead(Connection connection, Packet packet) throws IOException {
        write(connection, packet);
        return client.getInRunnable().reader.readPacket(connection);
    }

    void write(Connection connection, Packet packet) throws IOException {
        client.getOutRunnable().writer.write(connection, packet);
        client.getOutRunnable().writer.flush(connection);
    }
}
