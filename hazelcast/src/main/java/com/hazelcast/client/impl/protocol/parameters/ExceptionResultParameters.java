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
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

/**
 * ExceptionResultParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class ExceptionResultParameters {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.EXCEPTION;
    public String className;
    public String causeClassName;
    public String message;
    public String stacktrace;

    private ExceptionResultParameters(ClientMessage flyweight) {
        className = flyweight.getStringUtf8();
        causeClassName = flyweight.getStringUtf8();
        message = flyweight.getStringUtf8();
        stacktrace = flyweight.getStringUtf8();
    }

    public static ExceptionResultParameters decode(ClientMessage flyweight) {
        return new ExceptionResultParameters(flyweight);
    }

    public static ClientMessage encode(String className, String causeClassName, String message, String stacktrace) {
        final int requiredDataSize = calculateDataSize(className, causeClassName, message, stacktrace);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(className).set(causeClassName).set(message).set(stacktrace);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(String className, String causeClassName,  String message, String stacktrace) {
        return ClientMessage.HEADER_SIZE//
                + ParameterUtil.calculateStringDataSize(className)
                + ParameterUtil.calculateStringDataSize(causeClassName)
                + ParameterUtil.calculateStringDataSize(message)
                + ParameterUtil.calculateStringDataSize(stacktrace);
    }

}
