package com.hazelcast.client;

import com.hazelcast.core.ClientType;

/**
 * @author mdogan 7/3/13
 */
final class ClientExceptionConverters {

    private static final JavaClientExceptionConverter JAVA = new JavaClientExceptionConverter();
    private static final GenericClientExceptionConverter GENERIC = new GenericClientExceptionConverter();

    static ClientExceptionConverter get(ClientType type) {
        if (type == ClientType.JAVA) {
            return JAVA;
        }
        return GENERIC;
    }

}
