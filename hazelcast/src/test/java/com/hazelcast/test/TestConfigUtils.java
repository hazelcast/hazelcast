package com.hazelcast.test;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;

public final class TestConfigUtils {
    public static final int NON_DEFAULT_BACKUP_COUNT = MapConfig.MAX_BACKUP_COUNT;
    public static final InMemoryFormat NON_DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.OBJECT;

    private TestConfigUtils() {

    }
}
