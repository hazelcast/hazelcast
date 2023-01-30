/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public final class SqlConnectorUtil {

    private SqlConnectorUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T extends SqlConnector> T getJetSqlConnector(Table table) {
        SqlConnector connector;
        if (table instanceof JetTable) {
            connector = ((JetTable) table).getSqlConnector();
        } else if (table instanceof PartitionedMapTable) {
            connector = IMapSqlConnector.INSTANCE;
        } else {
            throw new JetException("Unknown table type: " + table.getClass());
        }
        return (T) connector;
    }

    public static int asInt(
            @Nonnull Map<String, String> options,
            @Nonnull String propertyName,
            @Nullable Integer defaultValue
    ) {
        String value = options.get(propertyName);
        if (value == null) {
            if (defaultValue == null) {
                throw QueryException.error("Missing property: " + propertyName);
            }
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw QueryException.error("Cannot parse option '" + propertyName + "' as integer: '" + value + '\'');
        }
    }
}
