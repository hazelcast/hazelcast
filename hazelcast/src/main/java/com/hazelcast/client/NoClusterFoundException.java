package com.hazelcast.client;

import com.hazelcast.core.HazelcastException;

/**
 * A {@link HazelcastException} thrown when a client wants to connect to a cluster, but there are no member
 * found it can connect to.
 */
public class NoClusterFoundException extends HazelcastException {

    public NoClusterFoundException(String message) {
        super(message);
    }

    public NoClusterFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
