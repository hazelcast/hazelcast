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

package com.hazelcast.dataconnection.impl;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

public enum DatabaseDialect {

    H2,

    POSTGRESQL,

    MYSQL,

    MICROSOFT_SQL_SERVER,

    ORACLE,

    OTHER;

    public static DatabaseDialect resolveDialect(DatabaseMetaData databaseMetaData) throws SQLException {
        String databaseProductName = databaseMetaData.getDatabaseProductName()
                                                     .toUpperCase(Locale.ROOT)
                                                     .trim();

        switch (databaseProductName) {
            case "H2":
                return H2;

            case "POSTGRESQL":
                return POSTGRESQL;

            case "MYSQL":
                return MYSQL;

            case "MICROSOFT SQL SERVER":
                return MICROSOFT_SQL_SERVER;

            case "ORACLE":
                return ORACLE;

            default:
                return OTHER;
        }
    }
}
