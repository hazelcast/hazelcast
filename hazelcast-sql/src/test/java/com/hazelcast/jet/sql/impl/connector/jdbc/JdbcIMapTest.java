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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcIMapTest extends JdbcSqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Test
    public void insertIntoIMapSelectFromJdbc() throws Exception {
        String tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, 5);

        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute(
                "CREATE MAPPING my_map ( " +
                        "__key INT, " +
                        "id INT, " +
                        "name VARCHAR ) " +
                        "TYPE IMap " +
                        "OPTIONS (" +
                        "    'keyFormat' = 'int'," +
                        "    'valueFormat' = 'compact',\n" +
                        "    'valueCompactTypeName' = 'person'" +
                        ")"
        );

        execute("INSERT INTO my_map SELECT id AS __key,id,name FROM " + tableName);

        Map<Object, Object> map = instance().getMap("my_map");
        assertThat(map).hasSize(5);
        GenericRecord record = (GenericRecord) map.get(1);
        assertThat(record.getInt32("id")).isEqualTo(1);
        assertThat(record.getString("name")).isEqualTo("name-1");
    }
}
