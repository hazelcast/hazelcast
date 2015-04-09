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
    public String message;
    public String stacktrace;

    private ExceptionResultParameters(ClientMessage flyweight) {
        className = flyweight.getStringUtf8();
        message = flyweight.getStringUtf8();
        stacktrace = flyweight.getStringUtf8();
    }

    public static ExceptionResultParameters decode(ClientMessage flyweight) {
        return new ExceptionResultParameters(flyweight);
    }

    public static ClientMessage encode(String className, String message, String stacktrace) {
        final int requiredDataSize = calculateDataSize(className, message, stacktrace);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(className).set(message).set(stacktrace);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(String className, String message, String stacktrace) {
        return ClientMessage.HEADER_SIZE//
                + ParameterUtil.calculateStringDataSize(className)
                + ParameterUtil.calculateStringDataSize(message)
                + ParameterUtil.calculateStringDataSize(stacktrace);
    }

}
