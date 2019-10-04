/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessage.Frame;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.util.ClientMessageSplitter;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;
import static com.hazelcast.internal.nio.IOUtil.readFully;
import static com.hazelcast.internal.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.internal.util.StringUtil.UTF8_CHARSET;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class verifies that client protocol protection is able to filter large and fragmented messages for untrusted
 * connections.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class })
public class ClientMessageProtectionTest {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testLimitsRemovedAfterAValidAuthentication() throws IOException {
        Config config = smallInstanceConfig();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ClientMessage clientMessage = createAuthenticationMessage(hz, createPassword(3));

        InetSocketAddress address = getNode(hz).getLocalMember().getSocketAddress(EndpointQualifier.CLIENT);
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setSoTimeout(5000);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY_NEW.getBytes(UTF8_CHARSET));
                writeClientMessage(os, clientMessage);
                ClientMessage respMessage = readResponse(is);
                assertEquals(ClientAuthenticationCodec.RESPONSE_MESSAGE_TYPE, respMessage.getMessageType());
                ClientAuthenticationCodec.ResponseParameters authnResponse = ClientAuthenticationCodec
                        .decodeResponse(respMessage);
                assertEquals(AuthenticationStatus.AUTHENTICATED, AuthenticationStatus.getById(authnResponse.status));

                // the connection is now trusted, lets try bigger and fragmented messages
                ClientMessage authenticationMessage = createAuthenticationMessage(hz, createPassword(1024));
                writeClientMessage(os, authenticationMessage);
                respMessage = readResponse(is);
                assertEquals(ClientAuthenticationCodec.RESPONSE_MESSAGE_TYPE, respMessage.getMessageType());
                authnResponse = ClientAuthenticationCodec.decodeResponse(respMessage);
                assertEquals(AuthenticationStatus.AUTHENTICATED, AuthenticationStatus.getById(authnResponse.status));

                List<ClientMessage> subFrames = ClientMessageSplitter.getFragments(50, clientMessage);
                assertTrue(subFrames.size() > 1);
                for (ClientMessage frame : subFrames) {
                    writeClientMessage(os, frame);
                }
                respMessage = readResponse(is);
                assertEquals(ClientAuthenticationCodec.RESPONSE_MESSAGE_TYPE, respMessage.getMessageType());
                authnResponse = ClientAuthenticationCodec.decodeResponse(respMessage);
                assertEquals(AuthenticationStatus.AUTHENTICATED, AuthenticationStatus.getById(authnResponse.status));
            }
        }
    }

    @Test
    public void testMessageFraming() throws IOException {
        Config config = smallInstanceConfig();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ClientMessage clientMessage = createAuthenticationMessage(hz, createPassword(200));
        InetSocketAddress address = getNode(hz).getLocalMember().getSocketAddress(EndpointQualifier.CLIENT);
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setSoTimeout(5000);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY_NEW.getBytes(UTF8_CHARSET));
                List<ClientMessage> subFrames = ClientMessageSplitter.getFragments(50, clientMessage);
                assertTrue(subFrames.size() > 1);
                writeClientMessage(os, subFrames.get(0));
                expected.expect(SocketTimeoutException.class);
                readResponse(is);
            }
        }
    }

    @Test
    public void testExceededMessageSize() throws IOException {
        Config config = smallInstanceConfig();
        int limit = 800;
        config.setProperty(GroupProperty.CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES.getName(), Integer.toString(limit));
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        String password = createPassword(limit);
        ClientMessage clientMessage = createAuthenticationMessage(hz, password);
        InetSocketAddress address = getNode(hz).getLocalMember().getSocketAddress(EndpointQualifier.CLIENT);
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setSoTimeout(5000);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY_NEW.getBytes(UTF8_CHARSET));
                writeClientMessage(os, clientMessage);
                expected.expect(EOFException.class);
                readResponse(is);
            }
        }
    }

    @Test
    public void testNegativeFrameLength() throws IOException {
        Config config = smallInstanceConfig();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ClientMessage clientMessage = createAuthenticationMessage(hz, "");
        InetSocketAddress address = getNode(hz).getLocalMember().getSocketAddress(EndpointQualifier.CLIENT);
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setSoTimeout(5000);
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY_NEW.getBytes(UTF8_CHARSET));
                ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                // it should be enough to write just the first frame
                Frame frame = clientMessage.getStartFrame();
                buffer.putInt(Integer.MIN_VALUE);
                buffer.putShort((short) (frame.flags));
                buffer.put(frame.content);
                os.write(byteBufferToBytes(buffer));
                os.flush();
                expected.expect(EOFException.class);
                readResponse(is);
            }
        }
    }

    private String createPassword(int pwdLength) {
        return new String(new char[pwdLength]).replace('\0', 'a');
    }

    @Test
    @Ignore
    /**
     * Ignore until issue https://github.com/hazelcast/hazelcast/issues/15658 is resolved
     */
    public void testAccumulatedMessageSizeOverflow() throws IOException {
        Config config = smallInstanceConfig();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ClientMessage clientMessage = createAuthenticationMessage(hz, "");
        InetSocketAddress address = getNode(hz).getLocalMember().getSocketAddress(EndpointQualifier.CLIENT);
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            try (OutputStream os = socket.getOutputStream(); InputStream is = socket.getInputStream()) {
                os.write(CLIENT_BINARY_NEW.getBytes(UTF8_CHARSET));
                // it should be enough to write just the first frame
                byte[] firstFrameBytes = frameAsBytes(clientMessage.getStartFrame(), false);
                os.write(firstFrameBytes);
                ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE_OF_FRAME_LENGTH_AND_FLAGS);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                // try to cause the size accumulator overflow
                buffer.putInt(Integer.MAX_VALUE - firstFrameBytes.length + 1);
                ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
                // skip start frame
                iterator.next();
                Frame frame = iterator.next();
                buffer.putShort((short) frame.flags);
                os.write(byteBufferToBytes(buffer));
                os.flush();
                expected.expect(EOFException.class);
                readResponse(is);
            }
        }
    }

    private ClientMessage createAuthenticationMessage(HazelcastInstance hz, String passwd) {
        return ClientAuthenticationCodec.encodeRequest(hz.getConfig().getClusterName(), passwd, null, null, true,
                "FOO", (byte) 1, "abc", "xxx", new ArrayList<>(), -1, null);
    }

    private ClientMessage readResponse(InputStream is) throws IOException, EOFException {
        ClientMessage clientMessage = ClientMessage.createForEncode();
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
        ClientMessage respMessage = ClientMessage.createForDecode(clientMessage.getStartFrame());
        return respMessage;
    }

    private void writeClientMessage(OutputStream os, final ClientMessage clientMessage) throws IOException {
        for (ClientMessage.ForwardFrameIterator it = clientMessage.frameIterator(); it.hasNext();) {
            ClientMessage.Frame frame = it.next();
            os.write(frameAsBytes(frame, it.hasNext()));
        }
        os.flush();
    }

    private byte[] frameAsBytes(ClientMessage.Frame frame, boolean isLastFrame) {
        byte[] content = frame.content != null ? frame.content : new byte[0];
        int frameSize = content.length + SIZE_OF_FRAME_LENGTH_AND_FLAGS;
        ByteBuffer buffer = ByteBuffer.allocateDirect(frameSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(frameSize);
        if (isLastFrame) {
            buffer.putShort((short) frame.flags);
        } else {
            buffer.putShort((short) (frame.flags | IS_FINAL_FLAG));
        }
        buffer.put(content);
        return byteBufferToBytes(buffer);
    }

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        buffer.flip();
        byte[] requestBytes = new byte[buffer.limit()];
        buffer.get(requestBytes);
        return requestBytes;
    }

}
