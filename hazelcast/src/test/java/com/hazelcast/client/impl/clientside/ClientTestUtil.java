/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.SerializationService;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.CLUSTER_VERSION;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.SUBSET_MEMBER_GROUPS_INFO;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.GROUPS;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.VERSION;
import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;
import static com.hazelcast.internal.nio.IOUtil.readFully;

public final class ClientTestUtil {

    public static HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance hz) {
        HazelcastClientInstanceImpl impl = null;
        if (hz instanceof HazelcastClientProxy proxy) {
            impl = proxy.client;
        } else if (hz instanceof HazelcastClientInstanceImpl instanceImpl) {
            impl = instanceImpl;
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

    /**
     * Utility method to create key-value pairs containing
     * member group information
     * @param version the version of the pair
     * @param groups the groups.
     * @return the map of key-values.
     */
    public static Map<String, String> createKeyValuePairs(int version, Set<UUID>... groups) {
        Map<String, String> keyValuePairs = new HashMap<>();
        JSONArray groupsArray = new JSONArray();
        for (Set<UUID> group : groups) {
            JSONArray groupArray = new JSONArray();
            for (UUID member : group) {
                groupArray.put(member.toString());
            }
            groupsArray.put(groupArray);
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(GROUPS, groupsArray);
        jsonObject.put(VERSION, version);
        keyValuePairs.put(SUBSET_MEMBER_GROUPS_INFO, jsonObject.toString());
        keyValuePairs.put(CLUSTER_VERSION, Versions.V5_5.toString());
        return keyValuePairs;
    }
}
