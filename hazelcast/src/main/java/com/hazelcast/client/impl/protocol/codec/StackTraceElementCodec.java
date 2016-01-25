/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;

public final class StackTraceElementCodec {

    private StackTraceElementCodec() {
    }

    public static StackTraceElement decode(ClientMessage clientMessage) {
        String declaringClass = clientMessage.getStringUtf8();
        String methodName = clientMessage.getStringUtf8();
        boolean fileName_Null = clientMessage.getBoolean();
        String fileName = null;
        if (!fileName_Null) {
            fileName = clientMessage.getStringUtf8();
        }
        int lineNumber = clientMessage.getInt();
        return new StackTraceElement(declaringClass, methodName, fileName, lineNumber);
    }

    public static void encode(StackTraceElement stackTraceElement, ClientMessage clientMessage) {
        clientMessage.set(stackTraceElement.getClassName());
        clientMessage.set(stackTraceElement.getMethodName());

        String fileName = stackTraceElement.getFileName();
        boolean fileName_Null = (fileName == null);
        clientMessage.set(fileName_Null);
        if (!fileName_Null) {
            clientMessage.set(fileName);
        }
        clientMessage.set(stackTraceElement.getLineNumber());
    }

    public static int calculateDataSize(StackTraceElement stackTraceElement) {
        int dataSize = Bits.INT_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(stackTraceElement.getClassName());
        dataSize += ParameterUtil.calculateDataSize(stackTraceElement.getMethodName());
        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        String fileName = stackTraceElement.getFileName();
        boolean fileName_NotNull = fileName != null;
        if (fileName_NotNull) {
            dataSize += ParameterUtil.calculateDataSize(fileName);
        }
        return dataSize;
    }
}
