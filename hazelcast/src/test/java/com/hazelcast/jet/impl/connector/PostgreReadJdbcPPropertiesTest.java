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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.pipeline.JdbcPropertyKeys;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PostgreReadJdbcPPropertiesTest extends ReadJdbcPPropertiesTest {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        initializeBeforeClass(new PostgresDatabaseProvider());
    }

    @Test
    public void testFetchSize() {
        int fetchSize = 2;
        Properties properties = new Properties();
        properties.put(JdbcPropertyKeys.FETCH_SIZE, String.valueOf(fetchSize));
        properties.put(JdbcPropertyKeys.AUTO_COMMIT, "false");
        runTestFetchSize(properties, fetchSize);
    }

    @Test
    public void testInvalidFetchSize() {
        Properties properties = new Properties();
        // FETCH_SIZE should be a number in string format
        properties.put(JdbcPropertyKeys.FETCH_SIZE, "aa");
        assertThatThrownBy(() -> runTest(properties))
                .hasRootCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testInvalidAutoCommit() {
        Properties properties = new Properties();
        // AUTO_COMMIT should be a boolean in string format
        properties.put(JdbcPropertyKeys.AUTO_COMMIT, "1");
        assertThatThrownBy(() -> runTest(properties))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }
}
