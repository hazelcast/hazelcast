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

package com.hazelcast.client;

import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertNotContains;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class })
public class AuthenticationInformationLeakTest {

    private static byte serializationVersion;
    private HazelcastInstance instance;
    private String clusterName;

    @BeforeClass
    public static void setupClass() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        serializationVersion = ss.getVersion();
    }

    @Before
    public void setup() {
        clusterName = randomString();
        Config config = new Config();
        config.setClusterName(clusterName);
        instance = Hazelcast.newHazelcastInstance(config);
    }

    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testAuthenticationExceptionDoesNotLeakInfo() {
        AtomicReference<ClientMessage> res = new AtomicReference<>();
        assertTrueEventually(() -> {
            res.set(tryGettingKeyValueWithoutAuthentication());
            assertNotNull(res.get());
        });
        assertEquals(ErrorsCodec.EXCEPTION_MESSAGE_TYPE, res.get().getMessageType());
        ClientExceptionFactory factory = new ClientExceptionFactory(false, Thread.currentThread().getContextClassLoader());
        Throwable err = factory.createException(res.get());
        String message = err.getMessage();
        assertInstanceOf(AuthenticationException.class, err.getCause());
        assertContains(message, "must authenticate before any operation");
        String messageLowerCase = message.toLowerCase();
        assertNotContains(messageLowerCase, "connection");
        assertNotContains(messageLowerCase, "authenticated");
        assertNotContains(messageLowerCase, "creationTime");
        assertNotContains(messageLowerCase, "clientAttributes");
    }

    /**
     * This function uses a tcp socket to be able to write a message without authenticating.
     * <p>
     * In case of an authentication failure, the server sends an exception message and immediately closes the connection
     * without waiting the message to be written to the client connection. It's possible that we receive EOF from socket,
     * which will make getKeyValue() throw IOException. In any kind of IOException, value null will be returned.
     *
     * @return null if IOException happened, otherwise return the response received from the server as a ClientMessage
     */
    private ClientMessage tryGettingKeyValueWithoutAuthentication() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        InetSocketAddress endpoint = instance.getCluster().getLocalMember().getSocketAddress();
        try (Socket socket = new Socket()) {
            socket.setReuseAddress(true);
            socket.connect(endpoint);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
                return getKeyValue(ss, os, is);
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Test
    public void testFailedAuthenticationDoesNotLeakInfoCredentialsFailed() throws Exception {
        authenticateAndAssert(AuthenticationStatus.CREDENTIALS_FAILED, serializationVersion, true, clusterName);
    }

    @Test
    public void testFailedAuthenticationDoesNotLeakInfoSerializationVersionMismatch() throws Exception {
        authenticateAndAssert(AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH, (byte) (serializationVersion + 1), false, clusterName);
    }

    @Test
    public void testFailedAuthenticationDoesNotLeakInfoClientNotAllowed() throws Exception {
        // No client is allowed in the cluster
        getClientEngineImpl(instance).applySelector(ClientSelectors.none());
        authenticateAndAssert(AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER, serializationVersion, false, clusterName);
    }

    private void authenticateAndAssert(AuthenticationStatus status, byte serVersion, boolean useWrongClusterName, String clusterName) throws IOException {
        InetSocketAddress endpoint = instance.getCluster().getLocalMember().getSocketAddress();
        try (Socket socket = new Socket()) {
            socket.setReuseAddress(true);
            socket.connect(endpoint);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
                String clientClusterName = useWrongClusterName ? clusterName + 'a' : clusterName;
                ClientMessage res = authenticate(clientClusterName, serVersion, os, is);
                ClientAuthenticationCodec.ResponseParameters responseParameters = ClientAuthenticationCodec.decodeResponse(res);
                assertEquals(status.getId(), responseParameters.status);
                assertEquals(-1, responseParameters.partitionCount);
                assertEquals(-1, responseParameters.serializationVersion);
                assertEquals("", responseParameters.serverHazelcastVersion);
                assertNull(responseParameters.address);
                assertNull(responseParameters.memberUuid);
                assertNull(responseParameters.clusterId);
            }
        }
    }

    private ClientMessage authenticate(String clusterName, byte serVersion, OutputStream os, InputStream is)
            throws IOException {
        UUID uuid = new UUID(0, 0);
        ClientMessage msg = ClientAuthenticationCodec.encodeRequest(clusterName, null, null, uuid, "", serVersion, "", "", new ArrayList<>());
        ClientTestUtil.writeClientMessage(os, msg);

        ClientMessage res = ClientTestUtil.readResponse(is);
        assertEquals(ClientAuthenticationCodec.RESPONSE_MESSAGE_TYPE, res.getMessageType());
        return res;
    }

    private ClientMessage getKeyValue(SerializationService ss, OutputStream os, InputStream is)
            throws IOException {
        Data keyData = ss.toData("key");
        ClientMessage msg = MapGetCodec.encodeRequest("mapName", keyData, 0);
        msg.setPartitionId(getPartitionId(keyData));
        ClientTestUtil.writeClientMessage(os, msg);
        ClientMessage res = ClientTestUtil.readResponse(is);
        if (res.getMessageType() != ErrorsCodec.EXCEPTION_MESSAGE_TYPE) {
            assertEquals(MapGetCodec.RESPONSE_MESSAGE_TYPE, res.getMessageType());
        }
        return res;
    }

    private int getPartitionId(Data keyData) {
        int hash = keyData.getPartitionHash();
        return HashUtil.hashToIndex(hash, 271);
    }
}
