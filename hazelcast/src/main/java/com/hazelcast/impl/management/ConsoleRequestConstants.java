package com.hazelcast.impl.management;

public final class ConsoleRequestConstants {

    public static final byte STATE_OUT_OF_MEMORY = 0;
    public static final byte STATE_ACTIVE = 1;

    public static final int REQUEST_TYPE_LOGIN = 0;
    public static final int REQUEST_TYPE_CLUSTER_STATE = 1;
    public static final int REQUEST_TYPE_GET_THREAD_DUMP = 2;
    public static final int REQUEST_TYPE_EXECUTE_SCRIPT = 3;
    public static final int REQUEST_TYPE_EVICT_LOCAL_MAP = 4;
    public static final int REQUEST_TYPE_CONSOLE_COMMAND = 5;
    public static final int REQUEST_TYPE_MAP_CONFIG = 6;

    private ConsoleRequestConstants() {
    }
}
