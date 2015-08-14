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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.client.impl.protocol.codec.StackTraceElementCodec;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * ExceptionResultParameters
 */
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
        justification = "fields may be needed for diagnostic")
public class ExceptionResultParameters {

    /**
     * ClientMessageType of this message
     */
    public static final int TYPE = ResponseMessageConst.EXCEPTION;
    public int errorCode;
    public String message;
    public StackTraceElement[] stackTrace;
//    public String stackTrace;

    private ExceptionResultParameters(ClientMessage flyweight) {
        errorCode = flyweight.getInt();
        boolean message_isNull = flyweight.getBoolean();
        if (!message_isNull) {
            message = flyweight.getStringUtf8();
        }

//        stackTrace = flyweight.getStringUtf8();
        int stackTraceCount = flyweight.getInt();
        stackTrace = new StackTraceElement[stackTraceCount];
        for (int i = 0; i < stackTraceCount; i++) {
            stackTrace[i] = StackTraceElementCodec.decode(flyweight);
        }
    }

    public static ExceptionResultParameters decode(ClientMessage flyweight) {
        return new ExceptionResultParameters(flyweight);
    }

    public static ClientMessage encode(int errorCode, String message, StackTraceElement[] stackTrace) {
//    public static ClientMessage encode(int errorCode, String message, String stackTrace) {
        final int requiredDataSize = calculateDataSize(errorCode, message, stackTrace);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE);
        clientMessage.set(errorCode);
        boolean message_isNull = message == null;
        clientMessage.set(message_isNull);
        if (!message_isNull) {
            clientMessage.set(message);
        }
//        clientMessage.set(stackTrace);
        clientMessage.set(stackTrace.length);
        for (StackTraceElement stackTraceElement : stackTrace) {
            StackTraceElementCodec.encode(stackTraceElement, clientMessage);
        }
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
//    public static int calculateDataSize(int errorCode, String message, String stackTrace) {
    public static int calculateDataSize(int errorCode, String message, StackTraceElement[] stackTrace) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.INT_SIZE_IN_BYTES;
        if (message == null) {
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        } else {
            dataSize += ParameterUtil.calculateDataSize(message);
        }
//        dataSize += ParameterUtil.calculateDataSize(stackTrace);
        for (StackTraceElement stackTraceElement : stackTrace) {
            dataSize += StackTraceElementCodec.calculateDataSize(stackTraceElement);
        }
        return dataSize;
    }

}
