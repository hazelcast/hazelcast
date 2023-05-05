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

import com.hazelcast.core.HazelcastException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.util.PropertyElf;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class HikariTranslator {

    private static final Map<String, String> PROPERTY_MAP = new HashMap<>();

    private final AtomicInteger counter;
    private final String name;

    // The translation from HZ to Hikari properties
    static {
        PROPERTY_MAP.put(DataConnectionProperties.JDBC_URL, "jdbcUrl");
        PROPERTY_MAP.put(DataConnectionProperties.USER, "username");
        PROPERTY_MAP.put(DataConnectionProperties.PASSWORD, "password");
        PROPERTY_MAP.put(DataConnectionProperties.CONNECTION_TIMEOUT, "connectionTimeout");
        PROPERTY_MAP.put(DataConnectionProperties.IDLE_TIMEOUT, "idleTimeout");
        PROPERTY_MAP.put(DataConnectionProperties.KEEP_ALIVE_TIME, "keepaliveTime");
        PROPERTY_MAP.put(DataConnectionProperties.MAX_LIFETIME, "maxLifetime");
        PROPERTY_MAP.put(DataConnectionProperties.MINIMUM_IDLE, "minimumIdle");
        PROPERTY_MAP.put(DataConnectionProperties.MAXIMUM_POOL_SIZE, "maximumPoolSize");
    }

    public HikariTranslator(AtomicInteger counter, String name) {
        this.counter = counter;
        this.name = name;
    }

    public Properties translate(Properties source) {
        Properties hikariProperties = new Properties();

        Set<String> propertyNames = PropertyElf.getPropertyNames(HikariConfig.class);
        // Iterate over source Properties and translate from HZ to Hikari
        source.forEach((key, value) -> {
            if (!(key instanceof String)) {
                throw new HazelcastException("The key: " + key + " should be a String object");
            }
            String keyString = (String) key;
            String translatedKey = PROPERTY_MAP.get(keyString);
            if (translatedKey != null) {
                // We can translate
                hikariProperties.put(translatedKey, value);
            } else {
                // We can not translate
                if (propertyNames.contains(keyString)) {
                    // If HikariConfig provides a setter, then use it
                    hikariProperties.put(key, value);
                } else {
                    // Pass it as a DataSource property to HikariConfig
                    // Concat with prefix if necessary
                    if (!(keyString.startsWith("dataSource."))) {
                        hikariProperties.put("dataSource." + keyString, value);
                    } else {
                        hikariProperties.put(keyString, value);
                    }
                }
            }
        });

        if (!hikariProperties.containsKey("poolName")) {
            int cnt = counter.getAndIncrement();
            hikariProperties.put("poolName", "HikariPool-" + cnt + "-" + name);
        }
        return hikariProperties;
    }
}
