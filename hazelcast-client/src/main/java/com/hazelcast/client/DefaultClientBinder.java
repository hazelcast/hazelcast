/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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
        authProtocol(connection, credentials);

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

//    void authPacket(Connection connection, Credentials credentials) throws IOException {
//
//        Packet auth = new Packet();
//        auth.set("", ClusterOperation.CLIENT_AUTHENTICATE, new byte[0], toByte(credentials));
//        Packet packet = writeAndRead(connection, auth);
//        final Object response = toObject(packet.getValue());
//        logger.log(Level.FINEST, "auth response:" + response);
//        if (response instanceof Exception) {
//            throw new RuntimeException((Exception) response);
//        }
//        if (!Boolean.TRUE.equals(response)) {
//            throw new AuthenticationException("Client [" + connection + "] has failed authentication");
//        }
//    }

//    Packet writeAndRead(Connection connection, Packet packet) throws IOException {
//        write(connection, packet);
//        return client.getInRunnable().reader.readPacket(connection);
//    }

    void write(Connection connection, Protocol command) throws IOException {
        client.getOutRunnable().write(connection, command);
    }


    void authProtocol(Connection connection, Credentials credentials) throws IOException {

        String[] args = null;
        ByteBuffer[] bb = null;

        if(credentials instanceof UsernamePasswordCredentials){
            UsernamePasswordCredentials upCredentials = (UsernamePasswordCredentials) credentials;
            args = new String[]{upCredentials.getUsername(), upCredentials.getPassword()};
        }
        else{
            bb = new ByteBuffer[]{ByteBuffer.wrap(toByte(credentials))};
        }

        Protocol auth = new Protocol(null, Command.AUTH, args,bb);

        Protocol response = writeAndRead(connection, auth);

        logger.log(Level.FINEST, "auth response:" + response.command);
        if(!response.command.equals(Command.OK)){
            throw new AuthenticationException("Client [" + connection + "] has failed authentication.");
        }
    }

    Protocol writeAndRead(Connection connection, Protocol command) throws IOException {
        write(connection, command);
        return client.getInRunnable().reader.read(connection);
    }
}
