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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.datalink.JdbcDataLink;

import java.util.Locale;

public class DataLinksUtil {
    public static final String DUMMY_CONNECTOR_TYPE = "com.hazelcast.datalink.impl.DataLinkTestUtil$DummyDataLink";
    public static final String JDBC_CONNECTOR_TYPE = "JDBC";
    public static final String MONGO_CONNECTOR_TYPE = "MONGO";
    public static final String KAFKA_CONNECTOR_TYPE = "KAFKA";

    private DataLinksUtil() {
    }

    public static Class<?> dataLinkConnectorTypeToClass(String connectorType) {
        // For testing purposes
        if (connectorType.equals(DUMMY_CONNECTOR_TYPE)) {
            try {
                return Class.forName(DUMMY_CONNECTOR_TYPE);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        String type = connectorType.toUpperCase(Locale.ROOT);
        switch (type) {
            case JDBC_CONNECTOR_TYPE:
                return JdbcDataLink.class;
            case MONGO_CONNECTOR_TYPE:
            case KAFKA_CONNECTOR_TYPE:
                throw new UnsupportedOperationException("Data link " + type + " is not yet supported");

        }
        throw new AssertionError("Unreachable");
    }
}
