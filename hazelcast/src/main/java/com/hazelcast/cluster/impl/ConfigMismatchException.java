package com.hazelcast.cluster.impl;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown when 2 nodes want to join, but there configuration doesn't match
 */
public class ConfigMismatchException extends HazelcastException {
    public ConfigMismatchException(String message) {
        super(message);
    }
}
