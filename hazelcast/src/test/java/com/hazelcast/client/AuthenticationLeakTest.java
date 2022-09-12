/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;
import static com.hazelcast.internal.nio.IOUtil.readFully;
import static com.hazelcast.internal.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertNotContains;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({ SlowTest.class })
public class AuthenticationLeakTest {

    static byte serVersion;
    HazelcastInstance instance;
    String clusterName;

    @BeforeClass
    public static void setupClass() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        serVersion = ss.getVersion();
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
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testAuthenticationExceptionDoesNotLeakInfo() throws Exception {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 5701);
        try (Socket socket = new Socket()) {
            socket.setReuseAddress(true);
            socket.connect(endpoint);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
                getKeyValue(ss, os, is);
            }
        } catch (RuntimeException runtimeException) {
            String message = runtimeException.getMessage();
            assertContains(message, "AuthenticationException");
            assertContains(message, "must authenticate before any operation");
            assertNotContains(message, "connection");
            assertNotContains(message, "Connection");
        }
    }

    @Test
    public void testFailedAuthenticationDoesNotLeakInfoCredentialsFailed() throws Exception {
        authenticateAndAssert(AuthenticationStatus.CREDENTIALS_FAILED, serVersion, true, clusterName);
    }

    @Test
    public void testFailedAuthenticationDoesNotLeakInfoSerializationVersionMismatch() throws Exception {
        authenticateAndAssert(AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH, (byte) (serVersion + 1), false, clusterName);
    }

    @Test
    public void testFailedAuthenticationDoesNotLeakInfoClientNotAllowed() throws Exception {
        // No client is allowed in the cluster
        getClientEngineImpl(instance).applySelector(ClientSelectors.none());
        authenticateAndAssert(AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER, serVersion, false, clusterName);
    }

    private void authenticateAndAssert(AuthenticationStatus status, byte serVersion, boolean useWrongClusterName, String clusterName) throws IOException {
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 5701);
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
        writeClientMessage(os, msg);
        return readResponse(is, ClientAuthenticationCodec.RESPONSE_MESSAGE_TYPE);
    }

    private void getKeyValue(SerializationService ss, OutputStream os, InputStream is)
            throws IOException {
        Data keyData = ss.toData("key");
        ClientMessage msg = MapGetCodec.encodeRequest("mapName", keyData, 0);
        msg.setPartitionId(getPartitionId(keyData));
        writeClientMessage(os, msg);
        readResponse(is, MapGetCodec.RESPONSE_MESSAGE_TYPE);
    }

    private int getPartitionId(Data keyData) {
        int hash = keyData.getPartitionHash();
        return HashUtil.hashToIndex(hash, 271);
    }

    private void writeClientMessage(OutputStream os, final ClientMessage clientMessage) throws IOException {
        for (ClientMessage.ForwardFrameIterator it = clientMessage.frameIterator(); it.hasNext();) {
            ClientMessage.Frame frame = it.next();
            os.write(frameAsBytes(frame, !it.hasNext()));
        }
        os.flush();
    }

    private byte[] frameAsBytes(ClientMessage.Frame frame, boolean isLastFrame) {
        byte[] content = frame.content != null ? frame.content : new byte[0];
        int frameSize = content.length + SIZE_OF_FRAME_LENGTH_AND_FLAGS;
        ByteBuffer buffer = ByteBuffer.allocateDirect(frameSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(frameSize);
        if (!isLastFrame) {
            buffer.putShort((short) frame.flags);
        } else {
            buffer.putShort((short) (frame.flags | IS_FINAL_FLAG));
        }
        buffer.put(content);
        return byteBufferToBytes(buffer);
    }

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        upcast(buffer).flip();
        byte[] requestBytes = new byte[buffer.limit()];
        buffer.get(requestBytes);
        return requestBytes;
    }

    private ClientMessage readResponse(InputStream is, int expectedMsgType) throws IOException {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        int msgType;
        do {
            while (true) {
                ByteBuffer frameSizeBuffer = ByteBuffer.allocate(SIZE_OF_FRAME_LENGTH_AND_FLAGS);
                frameSizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
                readFully(is, frameSizeBuffer.array());
                int frameSize = frameSizeBuffer.getInt();
                int flags = frameSizeBuffer.getShort() & 0xffff;
                byte[] content = new byte[frameSize - SIZE_OF_FRAME_LENGTH_AND_FLAGS];
                readFully(is, content);
                clientMessage.add(new ClientMessage.Frame(content, flags));
                if (ClientMessage.isFlagSet(flags, IS_FINAL_FLAG)) {
                    break;
                }
            }
            msgType = clientMessage.getMessageType();
            if (msgType == ErrorsCodec.EXCEPTION_MESSAGE_TYPE) {
                List<ErrorHolder> err = ErrorsCodec.decode(clientMessage);
                throw new RuntimeException(err.get(0).getMessage());
            }
        } while (msgType != expectedMsgType);
        return clientMessage;
    }
}
