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
    public String className;
    public String causeClassName;
    public String message;
    public String stacktrace;

    private ExceptionResultParameters(ClientMessage flyweight) {
        className = flyweight.getStringUtf8();
        boolean causeClassName_isNull = flyweight.getBoolean();
        if (!causeClassName_isNull) {
            causeClassName = flyweight.getStringUtf8();
        }
        boolean message_isNull = flyweight.getBoolean();
        if (!message_isNull) {
            message = flyweight.getStringUtf8();
        }
        boolean stackTrace_isNull = flyweight.getBoolean();
        if (!stackTrace_isNull) {
            stacktrace = flyweight.getStringUtf8();
        }
    }

    public static ExceptionResultParameters decode(ClientMessage flyweight) {
        return new ExceptionResultParameters(flyweight);
    }

    public static ClientMessage encode(String className, String causeClassName, String message, String stacktrace) {
        final int requiredDataSize = calculateDataSize(className, causeClassName, message, stacktrace);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE);
        clientMessage.set(className);
        boolean causeClassName_isNull = causeClassName == null;
        clientMessage.set(causeClassName_isNull);
        if (!causeClassName_isNull) {
            clientMessage.set(causeClassName);
        }
        boolean message_isNull = message == null;
        clientMessage.set(message_isNull);
        if (!message_isNull) {
            clientMessage.set(message);
        }
        boolean stackTrace_isNull = stacktrace == null;
        clientMessage.set(stackTrace_isNull);
        if (!stackTrace_isNull) {
            clientMessage.set(stacktrace);
        }

        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(String className, String causeClassName, String message, String stacktrace) {
        int dataSize = ClientMessage.HEADER_SIZE + ParameterUtil.calculateStringDataSize(className);
        if (causeClassName == null) {
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        } else {
            dataSize += ParameterUtil.calculateStringDataSize(causeClassName);
        }
        if (message == null) {
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        } else {
            dataSize += ParameterUtil.calculateStringDataSize(message);
        }
        if (stacktrace == null) {
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        } else {
            dataSize += ParameterUtil.calculateStringDataSize(stacktrace);
        }
        return dataSize;
    }

}
