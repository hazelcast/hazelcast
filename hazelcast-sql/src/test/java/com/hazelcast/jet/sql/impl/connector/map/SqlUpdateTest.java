/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
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
        createMapping("test_map", int.class, int.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        checkUpdateCount("UPDATE test_map SET this = 100 WHERE __key = 2", 0);
        assertThat(map).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 1, 2, 100, 3, 3));
    }

    @Test
    public void update_fieldInTheBeginning() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field1 = 200 WHERE __key = 1", 0);
        assertThat(map).containsExactly(entry(1, new Value(200, 200L, "300")));
    }

    @Test
    public void update_fieldInBetween() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field2 = 100 WHERE __key = 1", 0);
        assertThat(map).containsExactly(entry(1, new Value(100, 100L, "300")));
    }

    @Test
    public void update_fieldInTheEnd() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = '400' WHERE __key = 1", 0);
        assertThat(map).containsExactly(entry(1, new Value(100, 200L, "400")));
    }

    @Test
    public void update_mixedOrderOfFields() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = '200', field1 = 400, field2 = 600 WHERE __key = 1", 0);
        assertThat(map).containsExactly(entry(1, new Value(400, 600L, "200")));
    }

    @Test
    public void update_complexExpression() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field2 = 4 * field1 WHERE __key = 1", 0);
        assertThat(map).containsExactly(entry(1, new Value(100, 400L, "300")));
    }

    @Test
    public void update_selfReferentialCast() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field1 = CAST(field3 AS INT) WHERE __key = 1", 0);
        assertThat(map).containsExactly(entry(1, new Value(300, 200L, "300")));
    }

    @Test
    public void update_byKeyOrKey() {
        createMapping("test_map", int.class, int.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        checkUpdateCount("UPDATE test_map SET this = this + 1 WHERE __key = 1 OR __key = 3", 0);
        assertThat(map).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 2, 2, 2, 3, 4));
    }

    @Test
    public void update_by_keyAndKey() {
        createMapping("test_map", int.class, int.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, 1);
        map.put(2, 2);

        checkUpdateCount("UPDATE test_map SET this = this + 1 WHERE __key = 1 AND __key = 2", 0);
        assertThat(map).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 1, 2, 2));
    }

    @Test
    public void update_dynamicParam() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = 'p-' || ? WHERE __key = 1", 0, "300");
        assertThat(map).containsExactly(entry(1, new Value(100, 200L, "p-300")));
    }

    @Test
    public void updateByNonKeyField() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = '600' WHERE field1 = 100", 0);
        assertThat(map).containsExactly(entry(1, new Value(100, 200L, "600")));
    }

    @Test
    public void update_complexKey() {
        createMapping("test_map", Key.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(new Key(1), new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field3 = CAST(3 + keyField AS VARCHAR), field2 = 2 + 1, field1 = 1 WHERE keyField = 1", 0);
        assertThat(map).containsExactly(entry(new Key(1), new Value(1, 3L, "4")));
    }

    @Test
    public void update_basedOnWholeKey() {
        createMapping("test_map", Key.class, int.class);
        Map<Key, Integer> map = instance().getMap("test_map");
        map.put(new Key(1), 1);

        checkUpdateCount("UPDATE test_map SET this = CASE WHEN __key IS NULL THEN 2 ELSE 3 END", 0);
        assertThat(map).containsExactly(entry(new Key(1), 3));
    }

    @Test
    public void update_basedOnWholeValue() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        checkUpdateCount("UPDATE test_map SET field1 = CASE WHEN this IS NULL THEN 2 ELSE 3 END", 0);
        assertThat(map).containsExactly(entry(1, new Value(3, 200L, "300")));
    }

    @Test
    public void update_all() {
        createMapping("test_map", int.class, int.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, 1);
        map.put(2, 2);
        checkUpdateCount("UPDATE test_map SET this = this + 1", 0);
        assertThat(map).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 2, 2, 3));
    }

    @Test
    public void update_allWithAlwaysTrueCondition() {
        createMapping("test_map", int.class, int.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, 1);
        map.put(2, 2);

        checkUpdateCount("UPDATE test_map SET this = this + 1 WHERE 1 = 1", 0);
        assertThat(map).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 2, 2, 3));
    }

    @Test
    public void when_updateKey_then_fails() {
        createMapping("test_map", int.class, int.class);
        instance().getMap("test_map").put(1, 1);

        assertThatThrownBy(() -> execute("UPDATE test_map SET __key = 2"))
                .hasMessageContaining("Cannot update '__key'");
    }

    @Test
    public void when_updateKeyField_then_fails() {
        createMapping("test_map", Key.class, int.class);
        instance().getMap("test_map").put(new Key(1), 1);

        assertThatThrownBy(() -> execute("UPDATE test_map SET keyField = 2"))
                .hasMessageContaining("Cannot update 'keyField'");
    }

    @Test
    public void when_updateKeyNotHiddenAndHasFields_then_fails() {
        createMapping("test_map", Key.class, int.class);
        Map<Key, Integer> map = instance().getMap("test_map");
        map.put(new Key(1), 1);

        execute(
                "CREATE OR REPLACE MAPPING test_map ("
                        + "__key OBJECT"
                        + ", this INT "
                        + ", keyField INT external name \"__key.keyField\""
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + '\'' + OPTION_KEY_FORMAT + "'='java'"
                        + ", '" + OPTION_KEY_CLASS + "'='" + Key.class.getName() + "'"
                        + ", '" + OPTION_VALUE_FORMAT + "'='int'"
                        + ")"
        );

        assertThatThrownBy(() -> execute("UPDATE test_map SET __key = null"))
                .hasMessageContaining("Cannot update '__key'");
    }

    @Test
    public void when_updateThis_then_fails() {
        createMapping("test_map", int.class, Value.class);
        Map<Object, Object> map = instance().getMap("test_map");
        map.put(1, new Value(100, 200L, "300"));

        assertThatThrownBy(() -> execute("UPDATE test_map SET this = null"))
                .hasMessageContaining("Cannot update 'this'");
    }

    @Test
    public void when_updateThisNotHiddenAndHasFields_then_fails() {
        createMapping("test_map", int.class, Value.class);
        Map<Integer, Value> map = instance().getMap("test_map");
        map.put(1, new Value(1, 2L, "3"));

        execute(
                "CREATE OR REPLACE MAPPING test_map ("
                        + "__key INT"
                        + ", this OBJECT "
                        + ", field1 INT"
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + '\'' + OPTION_KEY_FORMAT + "'='int'"
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'"
                        + ", '" + OPTION_VALUE_CLASS + "'='" + Value.class.getName() + "'"
                        + ")"
        );

        assertThatThrownBy(() -> execute("UPDATE test_map SET this = null"))
                .hasMessageContaining("Cannot update 'this'");
    }

    @Test
    public void when_updateValueToNull_then_fails() {
        createMapping("test_map", int.class, int.class);
        instance().getMap("test_map").put(1, 1);

        assertThatThrownBy(() -> execute("UPDATE test_map SET this = null"))
                .hasMessageContaining("Cannot assign null to value");
    }

    @Test
    public void when_updateUnknownMapping_then_fails() {
        assertThatThrownBy(() -> execute("UPDATE test_map SET __key = 1"))
                .hasMessageContaining("Object 'test_map' not found");
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
