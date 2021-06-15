/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.Objects;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlUpdateTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
    }

    @Test
    public void update() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, 1);

        checkUpdateCount("UPDATE test_map SET this = 100 WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(100);
    }

    @Test
    public void update_fieldInTheBeginning() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field1 = 200 WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(200, 200L, "300"));
    }

    @Test
    public void update_fieldInBetween() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field2 = 100 WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 100L, "300"));
    }

    @Test
    public void update_fieldInTheEnd() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = '400' WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200L, "400"));
    }

    @Test
    public void update_mixedOrderOfFields() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = '200', field1 = 400, field2 = 600 WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(400, 600L, "200"));
    }

    @Test
    public void update_complexExpression() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field2 = 4 * field1 WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 400L, "300"));
    }

    @Test
    public void update_selfReferentialCast() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field1 = CAST(field3 AS INT) WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(300, 200L, "300"));
    }

    @Test
    public void update_dynamicParam() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = 'p-' || ? WHERE __key = 1", 0, "300");
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200L, "p-300"));
    }

    @Test
    public void updateByNonKeyField() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = '600' WHERE field1 = 100", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200L, "600"));
    }

    @Test
    public void explicitMapping() {
        execute(
                "CREATE MAPPING test_map ("
                        + "field1 INT"
                        + ", field2 BIGINT"
                        + ", field3 VARCHAR"
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='int'"
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'"
                        + ", '" + OPTION_VALUE_CLASS + "'='" + Value.class.getName() + "'"
                        + ")"
        );

        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field2 = 100 WHERE __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 100L, "300"));
    }

    @Test
    public void update_complexKey() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(new Key(1), new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = CAST(3 + keyField AS VARCHAR), field2 = 2 + 1, field1 = 1 WHERE keyField = 1", 0);
        assertThat(testMap.get(new Key(1))).isEqualTo(new Value(1, 3L, "4"));
    }

    @Test
    public void update_basedOnWholeKey() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(new Key(1), 1);

        checkUpdateCount("UPDATE test_map SET this = CASE WHEN __key IS NULL THEN 2 ELSE 3 END", 0);
        assertThat(testMap.get(new Key(1))).isEqualTo(3);
    }

    @Test
    public void update_basedOnWholeValue() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field1 = CASE WHEN this IS NULL THEN 2 ELSE 3 END", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(3, 200L, "300"));
    }

    @Test
    public void update_all() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, 1);
        testMap.put(2, 2);

        checkUpdateCount("UPDATE test_map SET this = 100", 0);
        assertThat(testMap.get(1)).isEqualTo(100);
        assertThat(testMap.get(2)).isEqualTo(100);
    }

    @Test
    public void update_allWithAlwaysTrueCondition() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, 1);
        testMap.put(2, 2);

        checkUpdateCount("UPDATE test_map SET this = 100 WHERE 1 = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(100);
        assertThat(testMap.get(2)).isEqualTo(100);
    }

    @Test
    public void when_updateKey_then_throws() {
        instance().getMap("test_map").put(1, 1);

        assertThatThrownBy(() -> execute("UPDATE test_map SET __key = 2"))
                .hasMessageContaining("Cannot update key");
    }

    @Test
    public void when_updateKeyField_then_throws() {
        instance().getMap("test_map").put(new Key(1), 1);

        assertThatThrownBy(() -> execute("UPDATE test_map SET keyField = 2"))
                .hasMessageContaining("Cannot update key");
    }

    @Test
    public void when_updateWholeValue_then_throws() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200L, "300"));

        assertThatThrownBy(() -> execute("UPDATE test_map SET this = null"))
                .hasMessageContaining("Cannot update 'this' field");
    }

    @Test
    public void when_updateUnknownMapping_then_throws() {
        assertThatThrownBy(() -> execute("UPDATE test_map SET __key = 1"))
                .hasMessageContaining("Object 'test_map' not found");
    }

    @Test
    public void when_updateFromSelectIsUsed_then_throws() {
        instance().getMap("m1").put(1, 1);
        instance().getMap("m2").put(1, 2);

        assertThatThrownBy(() -> execute("UPDATE m1 SET __key = (select m2.this from m2 WHERE m1.__key = m2.__key)"))
                .hasMessageContaining("UPDATE FROM SELECT not supported");
    }

    private void checkUpdateCount(String sql, int expected, Object... params) {
        assertThat(execute(sql, params).updateCount()).isEqualTo(expected);
    }

    private SqlResult execute(String sql, Object... params) {
        return instance().getSql().execute(sql, params);
    }

    public static class Key implements Serializable {

        public int keyField;

        @SuppressWarnings("unused")
        public Key() {
        }

        public Key(int keyField) {
            this.keyField = keyField;
        }

        @Override
        public String toString() {
            return "Key{" +
                    "keyField=" + keyField +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key value = (Key) o;
            return keyField == value.keyField;
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyField);
        }
    }

    public static class Value implements Serializable {

        public int field1;
        public long field2;
        public String field3;

        @SuppressWarnings("unused")
        public Value() {
        }

        public Value(int field1, long field2, String field3) {
            this.field1 = field1;
            this.field2 = field2;
            this.field3 = field3;
        }

        @Override
        public String toString() {
            return "Value{" +
                    "field1=" + field1 +
                    ", field2=" + field2 +
                    ", field3=" + field3 +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value value = (Value) o;
            return field1 == value.field1 && field2 == value.field2 && Objects.equals(field3, value.field3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2, field3);
        }
    }
}
