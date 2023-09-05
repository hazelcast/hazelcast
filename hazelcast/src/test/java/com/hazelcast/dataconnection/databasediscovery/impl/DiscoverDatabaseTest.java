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

package com.hazelcast.dataconnection.databasediscovery.impl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DiscoverDatabaseTest {

    @Test
    void isPostgres() {
        assertThat(DiscoverDatabase
                .isPostgres("jdbc:postgresql://localhost:3306/mydatabase?user=root&password=secret"))
                .isTrue();
        assertThat(DiscoverDatabase
                .isPostgres("jdbc:tc:postgresql://localhost:3306/mydatabase?user=root&password=secret"))
                .isTrue();

        assertThat(DiscoverDatabase
                .isPostgres("jdbc:tc:mysql://localhost:3306/mydatabase?user=root&password=secret"))
                .isFalse();
        assertThat(DiscoverDatabase
                .isPostgres("jdbc:oracle:thin:@myoracle.db.server:1521:my_sid"))
                .isFalse();
    }

    @Test
    void isMySql() {
        assertThat(DiscoverDatabase
                .isMySql("jdbc:mysql://localhost:3306/mydatabase?user=root&password=secret"))
                .isTrue();
        assertThat(DiscoverDatabase
                .isMySql("jdbc:tc:mysql://localhost:3306/mydatabase?user=root&password=secret"))
                .isTrue();

        assertThat(DiscoverDatabase
                .isMySql("jdbc:tc:postgresql://localhost:3306/mydatabase?user=root&password=secret"))
                .isFalse();
        assertThat(DiscoverDatabase
                .isMySql("jdbc:oracle:thin:@myoracle.db.server:1521:my_sid"))
                .isFalse();
    }

    @Test
    void isSqlServer() {
        assertThat(DiscoverDatabase
                .isSqlServer("jdbc:sqlserver://localhost:3306/mydatabase?user=root&password=secret"))
                .isTrue();
        assertThat(DiscoverDatabase
                .isSqlServer("jdbc:tc:sqlserver://localhost:3306/mydatabase?user=root&password=secret"))
                .isTrue();

        assertThat(DiscoverDatabase
                .isSqlServer("jdbc:tc:mysql://localhost:3306/mydatabase?user=root&password=secret"))
                .isFalse();
        assertThat(DiscoverDatabase
                .isSqlServer("jdbc:oracle:thin:@myoracle.db.server:1521:my_sid"))
                .isFalse();
    }
}
