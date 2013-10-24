package com.hazelcast.nio;

import java.util.UUID;

/**
 * User: grant
 */
public class ProcessIDFactory {

    private static final String uuid = UUID.randomUUID().toString();

    public static String getUuid() {
        return uuid;
    }
}
