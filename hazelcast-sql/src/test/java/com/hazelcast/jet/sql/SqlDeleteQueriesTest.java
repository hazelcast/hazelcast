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

import static org.assertj.core.api.Assertions.assertThat;

public class SqlDeleteQueriesTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
    }

    @Test
    public void deleteBySingleKey() {
        put(1);
        checkUpdateCount("delete from test_map where __key = 1", 0);
        assertMapDoesNotContainKey(1);

        put(1);
        checkUpdateCount("delete from test_map where 1 = __key", 0);
        assertMapDoesNotContainKey(1);

        put(1);
        checkUpdateCount("delete from test_map where 2 = __key", 0);
        assertMapContainsKey(1);

        put(1, 1);
        checkUpdateCount("delete from test_map where __key = this", 0);
        assertMapDoesNotContainKey(1);
    }

    @Test
    public void deleteWithoutKeyInPredicate() {
        put(1, 1);
        checkUpdateCount("delete from test_map where this = 1", 0);
        assertMapDoesNotContainKey(1);

        put(1, 1);
        checkUpdateCount("delete from test_map where 1 = this", 0);
        assertMapDoesNotContainKey(1);

        put(1, new Person("name", 18));
        checkUpdateCount("delete from test_map where name = 'name' and age = 18", 0);
        assertMapDoesNotContainKey(1);
    }

    @Test
    public void deleteByKey_andAnotherFields() {
        put(1, new Person("name1", 18));
        checkUpdateCount("delete from test_map where __key = 1 and age = 18", 0);
        assertMapDoesNotContainKey(1);

        put(1, new Person("name1", 18));
        checkUpdateCount("delete from test_map where __key = 1 and age = 50", 0);
        assertMapContainsKey(1);
    }

    @Test
    public void deleteWithDisjunctionPredicate_whenOnlyKeysInPredicate() {
        put(1);
        put(2);
        checkUpdateCount("delete from test_map where __key = 1 or __key = 2", 0);
        assertMapDoesNotContainKey(1);
        assertMapDoesNotContainKey(2);
    }

    @Test
    public void deleteThatDoesNotCheckKeyForEquality_fails() {
        put(10);

        checkUpdateCount("delete from test_map where __key > 1", 0);
        assertMapDoesNotContainKey(10);
    }

    @Test
    public void dontDelete_whenKeyFieldOccursMoreThanOneWithConjunctionPredicate() {
        put(1);

        checkUpdateCount("delete from test_map where __key = 1 and __key = 2", 0);
        assertMapContainsKey(1);
    }

    @Test
    public void explicitMapping() {
        String name = randomName();
        execute(
                "create mapping " + name + " (\n"
                        + "__key INT,\n"
                        + "this INT\n"
                        + ")\n"
                        + "TYPE imap\n"
                        + "OPTIONS (\n"
                        + "'keyFormat' = 'int',\n"
                        + "'valueFormat' = 'int'\n"
                        + ")"
        );
        instance().getMap(name).put(1, 1);
        assertMapContainsKey(name, 1);
        execute("delete from " + name + " where __key = 1");
        assertMapDoesNotContainKey(name, 1);
    }

    private SqlResult execute(String sql) {
        return instance().getSql().execute(sql);
    }

    private void checkUpdateCount(String sql, int expected) {
        assertThat(execute(sql).updateCount()).isEqualTo(expected);
    }

    private void put(Object key, Object value) {
        IMap<Object, Object> map = instance().getMap("test_map");
        map.clear();
        map.put(key, value);
    }

    private void put(Object key) {
        put(key, key);
    }

    private void assertMapDoesNotContainKey(int key) {
        assertMapDoesNotContainKey("test_map", key);
    }

    private void assertMapDoesNotContainKey(String mapName, int key) {
        IMap<Object, Object> test_map = instance().getMap(mapName);
        assertThat(test_map.containsKey(key)).isFalse();
    }

    private void assertMapContainsKey(int key) {
        assertMapContainsKey("test_map", key);
    }

    private void assertMapContainsKey(String mapName, int key) {
        IMap<Object, Object> test_map = instance().getMap(mapName);
        assertThat(test_map.containsKey(key)).isTrue();
    }

    public static class Person implements Serializable {
        public String name;
        public int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
