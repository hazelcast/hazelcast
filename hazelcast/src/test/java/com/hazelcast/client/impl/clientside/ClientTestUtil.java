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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;
import static com.hazelcast.internal.nio.IOUtil.readFully;

public final class ClientTestUtil {

    public static HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance hz) {
        HazelcastClientInstanceImpl impl = null;
        if (hz instanceof HazelcastClientProxy) {
            impl = ((HazelcastClientProxy) hz).client;
        } else if (hz instanceof HazelcastClientInstanceImpl) {
            impl = (HazelcastClientInstanceImpl) hz;
        }
        return impl;
    }

    public static SerializationService getClientSerializationService(HazelcastInstance hz) {
        return getHazelcastClientInstanceImpl(hz).getSerializationService();
    }

    public static void writeClientMessage(OutputStream os, final ClientMessage clientMessage) throws IOException {
        for (ClientMessage.ForwardFrameIterator it = clientMessage.frameIterator(); it.hasNext(); ) {
            ClientMessage.Frame frame = it.next();
            os.write(frameAsBytes(frame, !it.hasNext()));
        }
        os.flush();
    }

    public static byte[] frameAsBytes(ClientMessage.Frame frame, boolean isLastFrame) {
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
        return TestUtil.byteBufferToBytes(buffer);
    }

    public static ClientMessage readResponse(InputStream is) throws IOException {
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
        return clientMessage;
    }
}
