package com.hazelcast.client.impl.protocol.exception;

import com.hazelcast.core.HazelcastException;

/**
 * Created by ihsan on 15/12/15.
 */
public class MaxMessageSizeExceeded
        extends HazelcastException {
    public MaxMessageSizeExceeded() {
        super("The size of the message exceeds the maximum value of " + Integer.MAX_VALUE + " bytes.");
    }
}
