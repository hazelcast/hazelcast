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

package com.hazelcast.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientResultTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SQL_GOOD = "SELECT * FROM " + MAP_NAME;
    private static final String SQL_BAD = "SELECT * FROM " + MAP_NAME + "_bad";

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance member;
    private HazelcastInstance client;

    @Before
    public void before() {
        Config config = smallInstanceConfig();
        config.getSerializationConfig()
                .addDataSerializableFactory(TestDataSerializableFactory.ID, new TestDataSerializableFactory());
        member = factory.newHazelcastInstance(config);

        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testQuery() {
        Map<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(0, 0);
        map.put(1, 1);

        try (SqlResult result = execute(SQL_GOOD)) {
            assertEquals(2, result.getRowMetadata().getColumnCount());

            assertEquals(-1, result.updateCount());
            assertTrue(result.isRowSet());

            Iterator<SqlRow> iterator = result.iterator();
            iterator.next();
            iterator.next();
            assertFalse(iterator.hasNext());

            checkIllegalStateException(result::iterator, "Iterator can be requested only once");
        }
    }

    @Test
    public void testBadQuery() {
        Map<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(0, 0);
        map.put(1, 1);

        try (SqlResult result = execute(SQL_BAD)) {
            checkSqlException(result::iterator, SqlErrorCode.PARSING, "Object 'map_bad' not found");
            checkSqlException(result::getRowMetadata, SqlErrorCode.PARSING, "Object 'map_bad' not found");
            checkSqlException(result::updateCount, SqlErrorCode.PARSING, "Object 'map_bad' not found");
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testCloseBetweenFetches() {
        Map<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(0, 0);
        map.put(1, 1);

        try (SqlResult result = execute(SQL_GOOD)) {
            assertEquals(2, result.getRowMetadata().getColumnCount());

            assertEquals(-1, result.updateCount());
            assertTrue(result.isRowSet());

            Iterator<SqlRow> iterator = result.iterator();
            iterator.next();

            result.close();

            checkSqlException(iterator::hasNext, SqlErrorCode.CANCELLED_BY_USER, "Query was cancelled by the user");
            checkSqlException(iterator::next, SqlErrorCode.CANCELLED_BY_USER, "Query was cancelled by the user");
        }
    }

    @Test
    public void test_lazyDeserialization() {
        Map<Integer, Person> map = member.getMap(MAP_NAME);
        map.put(1, new Person("Alice", new Address()));

        try (SqlResult result = execute("SELECT * FROM " + MAP_NAME)) {
            SqlRow row = result.iterator().next();
            assertEquals(1, (int) row.getObject("__key"));
            assertEquals("Alice", row.getObject("name"));
            assertThatThrownBy(() -> row.getObject("address"))
                    .isInstanceOf(HazelcastSqlException.class)
                    .hasMessageContaining("Failed to deserialize query result value");
        }
    }

    private void checkSqlException(Runnable task, int expectedCode, String expectedMessage) {
        HazelcastSqlException err = assertThrows(HazelcastSqlException.class, task);

        assertEquals(expectedCode, err.getCode());
        assertTrue(err.getMessage(), err.getMessage().contains(expectedMessage));
    }

    private void checkIllegalStateException(Runnable task, String expectedMessage) {
        IllegalStateException err = assertThrows(IllegalStateException.class, task);

        assertEquals(expectedMessage, err.getMessage());
    }

    private SqlResult execute(String sql) {
        return client.getSql().execute(new SqlStatement(sql).setCursorBufferSize(1));
    }

    private static final class TestDataSerializableFactory implements DataSerializableFactory {

        private static final int ID = 1;

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == Person.ID) {
                return new Person();
            } else {
                assert typeId == Address.ID;
                return new Address();
            }
        }
    }

    public static final class Person implements IdentifiedDataSerializable {

        private static final int ID = 1;

        public String name;
        public Address address;

        private Person() {
        }

        private Person(String name, Address address) {
            this.name = name;
            this.address = address;
        }

        @Override
        public int getFactoryId() {
            return TestDataSerializableFactory.ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeObject(address);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readString();
            address = in.readObject();
        }
    }

    public static final class Address implements IdentifiedDataSerializable {

        private static final int ID = 2;

        @Override
        public int getFactoryId() {
            return TestDataSerializableFactory.ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }
}
