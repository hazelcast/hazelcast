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

import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;

public final class DataLinksUtil {
    public static final String DUMMY_DATA_LINK_TYPE = "com.hazelcast.datalink.impl.DataLinkTestUtil$DummyDataLink";

    private DataLinksUtil() {
    }

    public static Class<?> dataLinkConnectorTypeToClass(SqlConnectorCache connectorCache, String connectorType) {
        // Note : DUMMY_DATA_LINK_TYPE exists only for testing purposes.
        // Implementor have an intent not to expose dummy data link in syntax,
        // so there is a special case when we use simple dummy data link to test
        // correctness of SQL data link support. Dummy data link exists only in
        // test environment, an attempt to use it in real environment just return
        // a usual exception.
        if (connectorType.equals(DUMMY_DATA_LINK_TYPE)) {
            try {
                return Class.forName(DUMMY_DATA_LINK_TYPE);
            } catch (ClassNotFoundException e) {
                throw new UnsupportedOperationException("Data link class is not " +
                        "available for connector type" + DUMMY_DATA_LINK_TYPE);
            }
        }
        return connectorCache.forType(connectorType).dataLinkClass();
    }
}
