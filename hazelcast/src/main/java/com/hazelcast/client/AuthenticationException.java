package com.hazelcast.client;

import com.hazelcast.core.HazelcastException;

/**
 * @author mdogan 7/3/13
 */
public class AuthenticationException extends HazelcastException {

    public AuthenticationException() {
        super("Wrong group name and password.");
    }

    public AuthenticationException(String message) {
        super(message);
    }
}
