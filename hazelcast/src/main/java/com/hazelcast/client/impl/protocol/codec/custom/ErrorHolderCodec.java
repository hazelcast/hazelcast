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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("6a47cf9417a2b0ed3187e18b4d936eb2")
public final class ErrorHolderCodec {
    private static final int ERROR_CODE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ERROR_CODE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private ErrorHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.exception.ErrorHolder errorHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, ERROR_CODE_FIELD_OFFSET, errorHolder.getErrorCode());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, errorHolder.getClassName());
        CodecUtil.encodeNullable(clientMessage, errorHolder.getMessage(), StringCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, errorHolder.getStackTraceElements(), StackTraceElementCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.exception.ErrorHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int errorCode = decodeInt(initialFrame.content, ERROR_CODE_FIELD_OFFSET);

        java.lang.String className = StringCodec.decode(iterator);
        java.lang.String message = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.util.List<java.lang.StackTraceElement> stackTraceElements = ListMultiFrameCodec.decode(iterator, StackTraceElementCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.exception.ErrorHolder(errorCode, className, message, stackTraceElements);
    }
}
