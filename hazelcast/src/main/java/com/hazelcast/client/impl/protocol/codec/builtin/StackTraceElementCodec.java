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
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class StackTraceElementCodec {

    private static final int LINE_NUMBER = 0;
    private static final int INITIAL_FRAME_SIZE = LINE_NUMBER + Bits.INT_SIZE_IN_BYTES;

    private StackTraceElementCodec() {
    }

    public static void encode(ClientMessage clientMessage, StackTraceElement stackTraceElement) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeInt(initialFrame.content, LINE_NUMBER, stackTraceElement.getLineNumber());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, stackTraceElement.getClassName());
        StringCodec.encode(clientMessage, stackTraceElement.getMethodName());
        CodecUtil.encodeNullable(clientMessage, stackTraceElement.getFileName(), StringCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static StackTraceElement decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int lineNumber = FixedSizeTypesCodec.decodeInt(initialFrame.content, LINE_NUMBER);

        String declaringClass = StringCodec.decode(iterator);
        String methodName = StringCodec.decode(iterator);
        String fileName = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);
        return new StackTraceElement(declaringClass, methodName, fileName, lineNumber);
    }
}
