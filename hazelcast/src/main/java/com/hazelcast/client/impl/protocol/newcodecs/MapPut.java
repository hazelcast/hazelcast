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

package com.hazelcast.client.impl.protocol.newcodecs;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapMessageType;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.Iterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.CORRELATION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGEMENTED_MESSAGE;

public class MapPut {

    public static class Request {
        public String name;
        public Data key;
        public Data value;
        public long threadId;
        public long ttl;

        // Fixed fields offsets in header
        private static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        private static final int THREAD_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
        private static final int TTL_ID_FIELD_OFFSET = THREAD_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        private static final int HEADER_SIZE = TTL_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;

        public static final int TYPE = MapMessageType.MAP_PUT.id();

        public static ClientMessage encode(String name, Data key, Data value, long threadId, long ttl) {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            clientMessage.setRetryable(false);
            clientMessage.setAcquiresResource(false);
            clientMessage.setOperationName("Map.put");

            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGEMENTED_MESSAGE);
            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
            Bits.writeLongL(initialFrame.content, TTL_ID_FIELD_OFFSET, ttl);
            Bits.writeLongL(initialFrame.content, THREAD_ID_FIELD_OFFSET, threadId);

            clientMessage.addFrame(initialFrame);
            clientMessage.addFrame(new ClientMessage.Frame(name.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            clientMessage.addFrame(new ClientMessage.Frame(key.toByteArray(), DEFAULT_FLAGS));
            clientMessage.addFrame(new ClientMessage.Frame(value.toByteArray(), DEFAULT_FLAGS));

            return clientMessage;
        }

        public static Request decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
            Request request = new Request();

            ClientMessage.Frame initialFrame = iterator.next();
            request.threadId = Bits.readLongL(initialFrame.content, THREAD_ID_FIELD_OFFSET);
            request.ttl = Bits.readLongL(initialFrame.content, TTL_ID_FIELD_OFFSET);

            request.name = new String(iterator.next().content, Bits.UTF_8);
            request.key = new HeapData(iterator.next().content);
            request.value = new HeapData(iterator.next().content);
            return request;
        }
    }

    public static class Response {
        private static final int HEADER_SIZE = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        public static final int TYPE = 105;

        public Data value;

        public static ClientMessage encode(Data response) {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGEMENTED_MESSAGE);

            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
            clientMessage.addFrame(initialFrame);

            if (response == null) {
                clientMessage.addFrame(NULL_FRAME);
            } else {
                clientMessage.addFrame(new ClientMessage.Frame(response.toByteArray(), DEFAULT_FLAGS));
            }

            return clientMessage;
        }

        public static Response decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();

            Response response = new Response();

            ClientMessage.Frame frame = iterator.next();

            frame = iterator.next();
            if (!frame.isNullFrame()) {
                response.value = new HeapData(frame.content);
            }
            return response;
        }
    }

}
