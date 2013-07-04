package com.hazelcast.client;

/**
 * @author mdogan 7/3/13
 */
final class JavaClientExceptionConverter implements ClientExceptionConverter {

    public Object convert(Throwable t) {
        return t;
    }
}
