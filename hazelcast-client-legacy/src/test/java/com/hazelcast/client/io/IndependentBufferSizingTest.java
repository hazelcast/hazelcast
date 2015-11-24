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

package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingSocketReader;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingSocketWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IndependentBufferSizingTest extends HazelcastTestSupport {

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientBufferSize_SameAsForMemberMemberConnectionsByDefault() {
        Config config = new Config();
        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        TcpIpConnection clientConnection = getClientConnection(server);
        NonBlockingSocketReader reader = (NonBlockingSocketReader) clientConnection.getSocketReader();
        NonBlockingSocketWriter writer = (NonBlockingSocketWriter) clientConnection.getSocketWriter();

        int defaultReceiveBuffer = getDefaultReceiverBufferSize(server);
        int defaultSendBuffer = getDefaultSendBufferSize(server);

        assertHasByteBufferWithSize(reader, "inputBuffer", defaultReceiveBuffer);
        assertHasByteBufferWithSize(writer, "outputBuffer", defaultSendBuffer);
    }

    @Test
    public void testClientBufferSize_ExplicitOverride() {
        int receiveBufferSizeKB = 16;
        int sendBufferSizeKB = 4;

        Config config = new Config();
        config.setProperty(GroupProperty.SOCKET_CLIENT_RECEIVE_BUFFER_SIZE, Integer.toString(receiveBufferSizeKB));
        config.setProperty(GroupProperty.SOCKET_CLIENT_SEND_BUFFER_SIZE, Integer.toString(sendBufferSizeKB));

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        TcpIpConnection clientConnection = getClientConnection(server);
        NonBlockingSocketReader reader = (NonBlockingSocketReader) clientConnection.getSocketReader();
        NonBlockingSocketWriter writer = (NonBlockingSocketWriter) clientConnection.getSocketWriter();

        assertHasByteBufferWithSize(reader, "inputBuffer", receiveBufferSizeKB * 1024);
        assertHasByteBufferWithSize(writer, "outputBuffer", sendBufferSizeKB * 1024);
    }

    private int getDefaultSendBufferSize(HazelcastInstance instance) {
        Node node = getNode(instance);
        return node.getGroupProperties().getInteger(GroupProperty.SOCKET_SEND_BUFFER_SIZE) * 1024;
    }

    private int getDefaultReceiverBufferSize(HazelcastInstance instance) {
        Node node = getNode(instance);
        return node.getGroupProperties().getInteger(GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE) * 1024;
    }

    private TcpIpConnection getClientConnection(HazelcastInstance server) {
        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) getConnectionManager(server);
        Set<TcpIpConnection> activeConnections = connectionManager.getActiveConnections();
        return activeConnections.iterator().next();
    }

    private void assertHasByteBufferWithSize(Object object, String fieldName, int size) {
        ByteBuffer byteBuffer = getField(object, fieldName);
        assertEquals(size, byteBuffer.capacity());
    }

    private <T> T getField(Object object, String fieldName) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Class " + object.getClass() + " doesn't have a field " + fieldName + " declared");
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
