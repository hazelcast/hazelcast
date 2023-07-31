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

package com.hazelcast.jet.sql.impl.connector.jdbc.util;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class ResultSetUtils {

    private ResultSetUtils() {
    }

    public static Object[] getValueArray(ResultSet resultSet) throws SQLException {
        return new Object[resultSet.getMetaData().getColumnCount()];
    }

    public static void fillValues(ResultSet resultSet, Object[] values) throws SQLException {
        for (int index = 0; index < values.length; index++) {
            values[index] = resultSet.getObject(index + 1);
        }
    }
}
