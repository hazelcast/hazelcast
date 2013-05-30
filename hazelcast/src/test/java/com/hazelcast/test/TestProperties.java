package com.hazelcast.test;

/**
 * @mdogan 5/29/13
 */
public final class TestProperties {

    public static final String HAZELCAST_TEST_USE_NETWORK = "hazelcast.test.use.network";
    public static final String HAZELCAST_TEST_USE_CLIENT = "hazelcast.test.use.client";

    public static boolean isMockNetwork() {
        return !Boolean.getBoolean(HAZELCAST_TEST_USE_NETWORK);
    }

    public static boolean isUseClient() {
        return Boolean.getBoolean(HAZELCAST_TEST_USE_CLIENT);
    }

}
