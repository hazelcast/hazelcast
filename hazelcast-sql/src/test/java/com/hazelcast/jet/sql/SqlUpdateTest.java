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

package com.hazelcast.jet.sql;

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

public class SqlUpdateTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
    }

    @Test
    public void updateBySingleKey() {
        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, 1);
        checkUpdateCount("update test_map set this = cast(100 as integer) where __key = 1", 0);

        assertThat(testMap.get(1)).isEqualTo(100);
    }

    @Test
    public void updateBySingleKey_fieldInTheBeginning() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field1 = cast(200 as integer) where __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(200, 200, 300));
    }

    @Test
    public void updateBySingleKey_fieldInBetween() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field2 = cast(100 as integer) where __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 100, 300));
    }

    @Test
    public void updateBySingleKey_fieldInTheEnd() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field3 = cast(400 as integer) where __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200, 400));
    }

    @Test
    public void updateBySingleKey_mixedOrderOfFields() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount(
                "update test_map set\n"
                + "field3 = cast(200 as integer),\n"
                + "field1 = cast(400 as integer),\n"
                + "field2 = cast(600 as integer)\n"
                + "where __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(400, 600, 200));
    }

    @Test
    public void updateBySingleKey_complexExpression() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field3 = cast(2 * field3 as integer) where __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200, 600));
    }

    @Test
    public void updateBySingleKey_withParameters() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field3 = cast(? as integer) where __key = 1", 0, 1);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200, 1));
    }

    @Test
    public void updateByNonKeyField() {
        IMap<Object, Object> testMap = instance().getMap("test_map");

        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field3 = cast(2 * field3 as integer) where field1 = 100", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 200, 600));
    }

    @Test
    public void explicitMapping() {
        execute(
                "create mapping test_map (\n"
                        + "field1 INT,\n"
                        + "field2 INT,\n"
                        + "field3 INT\n"
                        + ") type imap\n"
                        + "OPTIONS (\n"
                        + '\'' + OPTION_KEY_FORMAT + "'='int',\n"
                        + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "',\n"
                        + '\'' + OPTION_VALUE_CLASS + "'='" + Value.class.getName() + "'\n"
                        + ")"
        );

        IMap<Object, Object> testMap = instance().getMap("test_map");
        testMap.put(1, new Value(100, 200, 300));
        checkUpdateCount("update test_map set field2 = cast(100 as integer) where __key = 1", 0);
        assertThat(testMap.get(1)).isEqualTo(new Value(100, 100, 300));
    }

    private void checkUpdateCount(String sql, int expected, Object... params) {
        assertThat(execute(sql, params).updateCount()).isEqualTo(expected);
    }

    private SqlResult execute(String sql, Object... params) {
        return instance().getSql().execute(sql, params);
    }

    public static class Value implements Serializable {
        public int field1;
        public int field2;
        public int field3;

        public Value() {
        }

        public Value(int field1, int field2, int field3) {
            this.field1 = field1;
            this.field2 = field2;
            this.field3 = field3;
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
            return field1 == value.field1 && field2 == value.field2 && field3 == value.field3;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2, field3);
        }

        @Override
        public String toString() {
            return "Value{" +
                    "field1=" + field1 +
                    ", field2=" + field2 +
                    ", field3=" + field3 +
                    '}';
        }
    }
}
