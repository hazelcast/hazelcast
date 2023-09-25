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

import com.hazelcast.internal.util.StringUtil;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static classloading.ThreadLeakTestUtils.getThreads;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class HikariTestUtil {
    private HikariTestUtil() {
    }

    public static void assertEventuallyNoHikariThreads(String configName) {
        assertTrueEventually("No Hikari threads", () -> assertThat(getThreads())
                .noneMatch(t -> t.getName().matches(hikariPoolRegex(configName))));
    }

    private static String hikariPoolRegex(String configName) {
        String suffix = StringUtil.isNullOrEmpty(configName) ? "" : "-" + configName;
        return "HikariPool-\\d+" + suffix;
    }

    public static void assertDataSourceClosed(DataSource closeableDataSource, String configName) {
        assertThatThrownBy(() -> executeQuery(closeableDataSource, "select 'some-name' as name"))
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("HikariDataSource HikariDataSource \\(" + hikariPoolRegex(configName) + "\\) has been closed.");
    }

    private static void executeQuery(DataSource dataSource, String sql) throws SQLException {
        try (PreparedStatement preparedStatement = dataSource.getConnection().prepareStatement(sql);
             ResultSet ignored = preparedStatement.executeQuery()) {
            //NOP
        }
    }

    public static void assertConnectionClosed(Connection connection, String configName) {
        assertThatThrownBy(() -> executeQuery(connection, "select 'some-name' as name"))
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("HikariDataSource HikariDataSource \\(" + hikariPoolRegex(configName) + "\\) has been closed.");
    }

    private static void executeQuery(Connection connection, String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet ignored = preparedStatement.executeQuery()) {
            //NOP
        }
    }

    public static void assertPoolNameEndsWith(DataSource dataSource, String suffix) throws SQLException {
        String poolName = dataSource.unwrap(HikariDataSource.class).getPoolName();
        assertThat(poolName).matches(hikariPoolRegex(suffix));
    }
}
