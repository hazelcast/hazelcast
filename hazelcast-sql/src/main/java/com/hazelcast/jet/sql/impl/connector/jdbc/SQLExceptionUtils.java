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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public final class SQLExceptionUtils {

    private SQLExceptionUtils() {
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    static boolean isNonTransientException(SQLException e) {
        SQLException next = e.getNextException();
        return e instanceof SQLNonTransientException
               || e.getCause() instanceof SQLNonTransientException
               || !isTransientCode(e.getSQLState())
               || (next != null && e != next && isNonTransientException(next));
    }

    private static boolean isTransientCode(String code) {
        if (code == null) {
            return false;
        }
        // Full list of error codes at:
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        switch (code) {
            // Sorted alphabetically
            case "08000":
            case "08001":
            case "08003":
            case "08004":
            case "08006":
            case "08007":
            case "40001":
            case "40P01":
            case "53000":
            case "53100":
            case "53200":
            case "53300":
            case "53400":
            case "55000":
            case "55006":
            case "55P03":
            case "57P03":
            case "58000":
            case "58030":
                return true;

            default:
                return false;
        }
    }
}
