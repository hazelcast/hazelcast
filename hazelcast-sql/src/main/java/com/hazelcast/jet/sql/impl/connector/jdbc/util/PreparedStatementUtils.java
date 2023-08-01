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

import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class PreparedStatementUtils {

    private PreparedStatementUtils() {
    }

    public static void setObjects(PreparedStatement preparedStatement, JetJoinInfo joinInfo, JetSqlRow leftRow)
            throws SQLException {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int index = 0; index < rightEquiJoinIndices.length; index++) {
            preparedStatement.setObject(index + 1, leftRow.get(index));
        }
    }
}
