package com.hazelcast.client;

/**
 * Thrown when Hazelcast client is not active during an invocation.
 */
public class HazelcastClientNotActiveException extends IllegalStateException {

    public HazelcastClientNotActiveException() {
        super("Hazelcast client is not active!");
    }

    public HazelcastClientNotActiveException(String message) {
        super(message);
    }
}

