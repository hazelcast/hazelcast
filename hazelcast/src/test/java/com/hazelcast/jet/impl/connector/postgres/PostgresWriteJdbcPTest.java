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

package com.hazelcast.jet.impl.connector.postgres;

import com.hazelcast.jet.impl.connector.WriteJdbcPTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.After;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresWriteJdbcPTest extends WriteJdbcPTest {
    @BeforeClass
    public static void beforeClass() {
        initialize(new PostgresDatabaseProvider()
                .withCommand("postgres -c max_prepared_transactions=10 -c max_connections=500"));
    }

    @After
    public void tearDown() throws Exception {
        listRemainingConnections();
    }

    private void listRemainingConnections() throws SQLException {
        try (
                Connection connection = ((DataSource) createDataSource(false)).getConnection();
                ResultSet resultSet = connection.createStatement().executeQuery(
                        "SELECT * FROM pg_stat_activity WHERE datname = current_database() and pid <> pg_backend_pid()")
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<String> rows = new ArrayList<>();
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                row.append(metaData.getColumnName(i)).append("\t|");
            }
            rows.add(row.toString());

            while (resultSet.next()) {
                row = new StringBuilder();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.append(resultSet.getObject(i)).append("\t|\t");
                }
                rows.add(row.toString());

            }

            if (!rows.isEmpty()) {
                logger.warning("Remaining connections: \n" + String.join("\n", rows));
            }
        }
    }
}
