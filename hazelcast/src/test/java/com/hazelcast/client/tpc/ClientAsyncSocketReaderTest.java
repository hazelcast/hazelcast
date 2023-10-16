/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.tpc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.protocol.codec.ClientTpcAuthenticationCodec;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.spi.properties.ClusterProperty.CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class})
public class ClientAsyncSocketReaderTest {

    private static final int UNVERIFIED_MESSAGE_LENGTH_LIMIT = 512;

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testProtocolBytesAndAuth() throws IOException {
        HazelcastInstance server = newServer();
        HazelcastInstance client = newClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = encodeAuthRequest(getUuid(client), getTpcToken(server));
            ClientTestUtil.writeClientMessage(os, request);
            ClientMessage response = ClientTestUtil.readResponse(is);
            assertEquals(ClientTpcAuthenticationCodec.RESPONSE_MESSAGE_TYPE, response.getMessageType());
        }
    }

    @Test
    public void testInvalidProtocolBytes() throws IOException {
        HazelcastInstance server = newServer();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write("!!!".getBytes(StandardCharsets.UTF_8));
            assertSocketClosed(is);
        }
    }

    @Test
    public void testValidProtocolBytes_whenSentPartByPart() throws IOException, InterruptedException {
        HazelcastInstance server = newServer();
        HazelcastInstance client = newClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            byte[] protocolBytes = Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8);
            os.write(protocolBytes, 0, 2);
            Thread.sleep(1_000);
            os.write(protocolBytes, 2, 1);

            ClientMessage request = encodeAuthRequest(getUuid(client), getTpcToken(server));
            ClientTestUtil.writeClientMessage(os, request);
            ClientMessage response = ClientTestUtil.readResponse(is);
            assertEquals(ClientTpcAuthenticationCodec.RESPONSE_MESSAGE_TYPE, response.getMessageType());
        }
    }

    @Test
    public void testMessageSizeLimits_beforeAuth() throws IOException {
        HazelcastInstance server = newServer();
        HazelcastInstance client = newClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = encodeAuthRequest(getUuid(client), new byte[UNVERIFIED_MESSAGE_LENGTH_LIMIT + 1]);
            ClientTestUtil.writeClientMessage(os, request);
            assertSocketClosed(is);
        }
    }

    @Test
    public void testMessageSizeLimits_afterAuth() throws IOException {
        HazelcastInstance server = newServer();
        HazelcastInstance client = newClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = encodeAuthRequest(getUuid(client), getTpcToken(server));
            ClientTestUtil.writeClientMessage(os, request);
            ClientMessage response = ClientTestUtil.readResponse(is);
            assertEquals(ClientTpcAuthenticationCodec.RESPONSE_MESSAGE_TYPE, response.getMessageType());

            ClientMessage largeRequest = encodeAuthRequest(getUuid(client), new byte[UNVERIFIED_MESSAGE_LENGTH_LIMIT + 1]);
            ClientTestUtil.writeClientMessage(os, largeRequest);
            ClientMessage largeResponse = ClientTestUtil.readResponse(is);
            assertEquals(ClientTpcAuthenticationCodec.RESPONSE_MESSAGE_TYPE, largeResponse.getMessageType());
        }
    }

    @Test
    public void testMessagesBeforeAuth() throws IOException {
        HazelcastInstance server = newServer();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = ClientPingCodec.encodeRequest();
            ClientTestUtil.writeClientMessage(os, request);
            assertSocketClosed(is);
        }
    }

    @Test
    public void testAuth_withNotFoundUuid() throws IOException {
        HazelcastInstance server = newServer();
        newClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = encodeAuthRequest(UUID.randomUUID(), getTpcToken(server));
            ClientTestUtil.writeClientMessage(os, request);
            assertSocketClosed(is);
        }
    }

    @Test
    public void testAuth_withMismatchedToken() throws IOException {
        HazelcastInstance server = newServer();
        HazelcastInstance client = newClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = encodeAuthRequest(getUuid(client), new byte[0]);
            ClientTestUtil.writeClientMessage(os, request);
            assertSocketClosed(is);
        }
    }

    @Test
    public void testAuth_withMismatchedToken_forTpcDisabledClient() throws IOException {
        HazelcastInstance server = newServer();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        try (Socket socket = newClientSocket(server);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream()) {

            os.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
            ClientMessage request = encodeAuthRequest(getUuid(client), new byte[0]);
            ClientTestUtil.writeClientMessage(os, request);
            assertSocketClosed(is);
        }
    }

    private void assertSocketClosed(InputStream is) throws IOException {
        assertEquals(-1, is.read());
    }

    private ClientMessage encodeAuthRequest(UUID uuid, byte[] token) {
        return ClientTpcAuthenticationCodec.encodeRequest(uuid, token);
    }

    private UUID getUuid(HazelcastInstance client) {
        return client.getLocalEndpoint().getUuid();
    }

    private Socket newClientSocket(HazelcastInstance server) throws IOException {
        String host = server.getCluster().getLocalMember().getAddress().getHost();
        int port = getNode(server).nodeEngine.getTpcServerBootstrap().getClientPorts().iterator().next();
        return new Socket(host, port);
    }

    private byte[] getTpcToken(HazelcastInstance server) {
        Collection<ClientEndpoint> endpoints = getNode(server).clientEngine.getEndpointManager().getEndpoints();
        return endpoints.iterator().next().getTpcToken().getContent();
    }

    private HazelcastInstance newServer() {
        Config config = HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics();
        config.getTpcConfig()
                .setEnabled(true)
                .setEventloopCount(1);
        config.setProperty(CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES.getName(), String.valueOf(UNVERIFIED_MESSAGE_LENGTH_LIMIT));
        return Hazelcast.newHazelcastInstance(config);
    }

    private HazelcastInstance newClient() {
        ClientConfig config = new ClientConfig();
        config.getTpcConfig().setEnabled(true);
        return HazelcastClient.newHazelcastClient(config);
    }
}
