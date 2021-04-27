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
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlDeleteQueriesTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
    }

    @Test
    public void deleteBySingleKey() {
        put(1);
        checkUpdateCount("delete from test_map where __key = 1", 1);
        put(1);
        checkUpdateCount("delete from test_map where 1 = __key", 1);
        put(1);
        checkUpdateCount("delete from test_map where 2 = __key", 0);
    }

    @Test
    public void fails_whenThereIsNoKeyInPredicate() {
        put(1);
        checkError("delete from test_map where this = 1", "DELETE query has to contain __key = <const value> predicate");
        checkError("delete from test_map where 1 = this", "DELETE query has to contain __key = <const value> predicate");
        checkError("delete from test_map where __key = this", "DELETE query has to contain __key = <const value> predicate");

        put(1, new Person("name", 18));
        checkError("delete from test_map where name = 'name' and age = 18", "DELETE query has to contain __key = <const value> predicate");
    }

    @Test
    public void deleteByKey_andAnotherFields() {
        IMap<Integer, Person> map = instance().getMap("people");
        map.clear();
        map.put(1, new Person("name1", 18));

        checkUpdateCount("delete from people where __key = 1 and age = 18", 1);

        map.put(1, new Person("name1", 18));
        checkUpdateCount("delete from people where __key = 1 and age = 50", 0);
    }

    @Test
    public void deleteWithDisjunctionPredicate_whenOnlyKeysInPredicate() {
        put(1);
        put(2);
        checkError("delete from test_map where __key = 1 or __key = 2", "Complex DELETE queries unsupported");
    }

    @Test
    public void deleteThatDoesNotCheckKeyForEquality_fails() {
        put(10);

        checkError("delete from test_map where __key > 1", "GREATER_THAN predicate is not supported for DELETE queries");
    }

    @Test
    public void dontDelete_whenKeyFieldOccursMoreThanOneWithConjunctionPredicate() {
        put(1);

        checkUpdateCount("delete from test_map where __key = 1 and __key = 2", 0);
    }

    private void checkError(String sql, String expectedErrorMessage) {
        try {
            instance().getSql().execute(sql);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertNotNull(e.getMessage());
            assertTrue(
                    "\nExpected: " + expectedErrorMessage + "\nActual: " + e.getMessage(),
                    e.getMessage().contains(expectedErrorMessage)
            );

            assertEquals(e.getCode() + ": " + e.getMessage(), SqlErrorCode.GENERIC, e.getCode());
        }
    }

    private void checkUpdateCount(String sql, int expected) {
        assertThat(instance().getSql().execute(sql).updateCount()).isEqualTo(expected);
    }

    private void put(Object key, Object value) {
        IMap<Object, Object> map = instance().getMap("test_map");
        map.clear();
        map.put(key, value);
    }

    private void put(Object key) {
        put(key, key);
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
