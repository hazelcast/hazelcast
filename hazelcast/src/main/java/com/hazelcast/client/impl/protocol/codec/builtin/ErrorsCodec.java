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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.custom.ErrorHolderCodec;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;

import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientMessage.RESPONSE_BACKUP_ACKS_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.BYTE_SIZE_IN_BYTES;

public final class ErrorsCodec {

    // Other codecs message types can be in range 0x000100 - 0xFFFFFF
    // So, it is safe to supply a custom message type for exceptions in
    // the range 0x000000 - 0x0000FF
    public static final int EXCEPTION_MESSAGE_TYPE = 0;
    private static final int INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private ErrorsCodec() {
    }

    public static ClientMessage encode(List<ErrorHolder> errorHolders) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        clientMessage.add(initialFrame);
        clientMessage.setMessageType(EXCEPTION_MESSAGE_TYPE);
        ListMultiFrameCodec.encode(clientMessage, errorHolders, ErrorHolderCodec::encode);
        return clientMessage;
    }

    public static List<ErrorHolder> decode(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        //initial frame
        iterator.next();
        return ListMultiFrameCodec.decode(iterator, ErrorHolderCodec::decode);
    }
}
