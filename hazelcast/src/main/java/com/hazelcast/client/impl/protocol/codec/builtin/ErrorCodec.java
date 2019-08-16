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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.nio.Bits;

import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class ErrorCodec {

    // Other codecs message types can be in range 0x000100 - 0xFFFFFF
    // So, it is safe to supply a custom message type for exceptions in
    // the range 0x000000 - 0x0000FF
    public static final int EXCEPTION_MESSAGE_TYPE = 0;
    private static final int ERROR_CODE = 0;
    private static final int INITIAL_FRAME_SIZE = ERROR_CODE + Bits.INT_SIZE_IN_BYTES;

    private ErrorCodec() {
    }

    public static void encode(ClientMessage clientMessage, ErrorHolder errorHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeInt(initialFrame.content, ERROR_CODE, errorHolder.getErrorCode());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, errorHolder.getClassName());
        CodecUtil.encodeNullable(clientMessage, errorHolder.getMessage(), StringCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, errorHolder.getStackTraceElements(), StackTraceElementCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static ErrorHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int errorCode = FixedSizeTypesCodec.decodeInt(initialFrame.content, ERROR_CODE);

        String className = StringCodec.decode(iterator);
        String message = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        List<StackTraceElement> stackTraceElements = ListMultiFrameCodec.decode(iterator, StackTraceElementCodec::decode);

        fastForwardToEndFrame(iterator);
        return new ErrorHolder(errorCode, className, message, stackTraceElements);
    }
}
