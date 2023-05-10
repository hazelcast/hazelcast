/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.dataconnection.impl.jdbcproperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class HikariTranslator {

    private static final String HIKARI_PREFIX = "hikari.";
    private static final Map<String, String> PROPERTY_MAP = new HashMap<>();

    private final AtomicInteger poolCounter;
    private final String name;

    // The translation from HZ to Hikari properties
    static {
        PROPERTY_MAP.put(DataConnectionProperties.JDBC_URL, "jdbcUrl");
        PROPERTY_MAP.put(DataConnectionProperties.CONNECTION_TIMEOUT, "connectionTimeout");
        PROPERTY_MAP.put(DataConnectionProperties.IDLE_TIMEOUT, "idleTimeout");
        PROPERTY_MAP.put(DataConnectionProperties.KEEP_ALIVE_TIME, "keepaliveTime");
        PROPERTY_MAP.put(DataConnectionProperties.MAX_LIFETIME, "maxLifetime");
        PROPERTY_MAP.put(DataConnectionProperties.MINIMUM_IDLE, "minimumIdle");
        PROPERTY_MAP.put(DataConnectionProperties.MAXIMUM_POOL_SIZE, "maximumPoolSize");
    }

    public HikariTranslator(AtomicInteger poolCounter, String name) {
        this.poolCounter = poolCounter;
        this.name = name;
    }

    public Properties translate(Properties source) {
        Properties hikariProperties = new Properties();

        // Iterate over source Properties and translate from HZ to Hikari
        source.forEach((key, value) -> {
            String keyString = (String) key;
            if (PROPERTY_MAP.containsKey(keyString)) {
                hikariProperties.put(keyString, value);
            } else if (keyString.startsWith(HIKARI_PREFIX)) {
                String keyNoPrefix = keyString.substring(HIKARI_PREFIX.length());
                hikariProperties.put(keyNoPrefix, source.get(keyString));
            } else {
                hikariProperties.put("dataSource." + keyString, value);
            }
        });

        int cnt = poolCounter.getAndIncrement();
        hikariProperties.put("poolName", "HikariPool-" + cnt + "-" + name);

        return hikariProperties;
    }
}
