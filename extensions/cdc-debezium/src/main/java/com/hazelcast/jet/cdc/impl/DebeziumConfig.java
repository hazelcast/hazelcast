/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc.impl;

import com.hazelcast.internal.util.UuidUtil;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

public class DebeziumConfig {

    private final Properties properties = new Properties();

    public DebeziumConfig(@Nonnull String name, @Nonnull String connectorClass) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(connectorClass, "connectorClass");

        properties.setProperty("name", name);
        properties.setProperty(ReadCdcP.CONNECTOR_CLASS_PROPERTY, connectorClass);
        properties.setProperty("tombstones.on.delete", "false");
        properties.setProperty("topic.prefix", String.valueOf(UuidUtil.newUnsecureUUID()));
    }

    public void setProperty(String key, Object value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null for key " + key);

        properties.put(key, value);
    }

    public void setProperty(String key, String value) {
        setProperty(key, (Object) value);
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.putAll(properties);
        return props;
    }
}
