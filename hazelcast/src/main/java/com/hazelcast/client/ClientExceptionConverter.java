package com.hazelcast.client;

/**
 * @author mdogan 7/3/13
 */
interface ClientExceptionConverter {

    Object convert(Throwable t);

}
