/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SqlConnectorCache {

    private static final String FACTORY_ID = "com.hazelcast.sql.Connectors";

    private final Map<String, SqlConnector> connectors = new HashMap<>();

    public SqlConnectorCache(NodeEngine nodeEngine) {
        try {
            ServiceLoader.iterator(SqlConnector.class, FACTORY_ID, nodeEngine.getConfigClassLoader())
                         .forEachRemaining(this::addConnector);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addConnector(SqlConnector connector) {
        String type = connector.typeName();
        String canonicalType = toCanonicalType(type);
        if (connectors.putIfAbsent(canonicalType, connector) != null) {
            throw new HazelcastException("Duplicate connector: " + type);
        }
    }

    public SqlConnector forType(String type) {
        String canonicalType = toCanonicalType(type);
        return requireNonNull(connectors.get(canonicalType), "Unknown connector type: " + type);
    }

    private static String toCanonicalType(String type) {
        return type.toUpperCase(Locale.ENGLISH);
    }
}
