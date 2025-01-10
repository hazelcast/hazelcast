/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.SqlConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;

public class UntimedStatementTest
        extends SqlTestSupport {

    @BeforeClass
    public static void before() {
        SqlConfig globalStatementTimeoutConfig = new SqlConfig().setStatementTimeoutMillis(1000);
        initializeWithClient(1, smallInstanceConfig().setSqlConfig(globalStatementTimeoutConfig), null);
    }

    @Test
    public void testTimeoutIgnoredForCreateMapping() {
        assertThatNoException().isThrownBy(() -> client().getSql().execute("""
                CREATE OR REPLACE MAPPING myMap
                TYPE IMap
                OPTIONS (
                    'keyFormat'='varchar',
                    'valueFormat'='int');
                """).close());
    }

    @Test
    public void testTimeoutIgnoredForDropMapping() {
        assertThatNoException().isThrownBy(() -> client().getSql().execute("DROP MAPPING IF EXISTS myMap").close());
    }

    @Test
    public void testTimeoutIgnoredForCreateDataConnection() {
        assertThatNoException().isThrownBy(() -> client().getSql().execute("""
                CREATE DATA CONNECTION IF NOT EXISTS myKafkaSource
                TYPE Kafka
                SHARED
                OPTIONS (
                    'bootstrap.servers' = '127.0.0.1:9092');
                """).close());
    }

    @Test
    public void testTimeoutIgnoredForDropDataConnection() {
        assertThatNoException().isThrownBy(
                () -> client().getSql().execute("DROP DATA CONNECTION IF EXISTS data_connection_name").close());
    }

    @Test
    public void testTimeoutIgnoredForCreateIndex() {
        assertThatNoException().isThrownBy(() -> {
            client().getSql().execute("""
                    CREATE OR REPLACE MAPPING myMap (
                        name varchar,
                        id INT
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat'='varchar',
                        'valueFormat'='json-flat');
                    """).close();

            client().getSql().execute("""
                    CREATE INDEX IF NOT EXISTS id
                    ON myMap (id)
                    TYPE SORTED;
                    """).close();
        });
    }

    @Test
    public void testTimeoutIgnoredForCreateJob() {
        assertThatNoException().isThrownBy(() -> {
            client().getSql().execute("""
                    CREATE OR REPLACE MAPPING myMap (
                        name varchar,
                        id INT
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat'='varchar',
                        'valueFormat'='json-flat');
                    """).close();

            client().getSql().execute("""
                    CREATE JOB myJob
                    AS
                    INSERT INTO myMap
                    SELECT * FROM myMap;
                    """).close();
        });
    }

    @Test
    public void testTimeoutIgnoredForDropJob() {
        assertThatNoException().isThrownBy(() -> client().getSql().execute("DROP JOB IF EXISTS test_job").close());
    }

    @Test
    public void testTimeoutIgnoredForCreateView() {
        assertThatNoException().isThrownBy(() -> {
            client().getSql().execute("""
                    CREATE OR REPLACE MAPPING myMap (
                        name varchar,
                        id INT
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat'='varchar',
                        'valueFormat'='json-flat');
                    """).close();

            client().getSql().execute("""
                    CREATE VIEW testView
                    AS
                    SELECT *
                    FROM myMap
                    WHERE id > 70;
                    """).close();
        });
    }

    @Test
    public void testTimeoutIgnoredForDropView() {
        assertThatNoException().isThrownBy(() -> client().getSql().execute("DROP VIEW IF EXISTS testView").close());
    }

    @Test
    public void testTimeoutIgnoredForShow() {
        assertThatNoException().isThrownBy(() -> client().getSql().execute("SHOW MAPPINGS").close());
    }

    @Test
    public void testTimeoutIgnoredForExplain() {
        assertThatNoException().isThrownBy(() -> {
            client().getSql().execute("""
                    CREATE OR REPLACE MAPPING myMap
                    TYPE IMap
                    OPTIONS (
                        'keyFormat'='varchar',
                        'valueFormat'='int');
                    """).close();

            client().getSql().execute("EXPLAIN SELECT * FROM myMap").close();
        });
    }
}
