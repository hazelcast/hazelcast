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

package com.hazelcast.sql.misc;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that ensures that there are no unexpected serializations when executing a query on an IMap with the
 * default {@link InMemoryFormat#BINARY} format.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(ParallelJVMTest.class)
public class SqlNoSerializationTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final int KEY_COUNT = 100;

    @Parameterized.Parameter
    public boolean useIndex;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance member;

    private static volatile boolean failOnSerialization;

    @Parameterized.Parameters(name = "useIndex:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(true, false);
    }

    @Before
    public void before() {
        member = factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        Map<Key, Value> localMap = new HashMap<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            localMap.put(new Key(i), new Value(i));
        }

        IMap<Key, Value> map = member.getMap(MAP_NAME);

        map.putAll(localMap);

        // An index may change the behavior due to MapContainer.isUseCachedDeserializedValuesEnabled.
        if (useIndex) {
            map.addIndex(new IndexConfig().setType(IndexType.SORTED).addAttribute("val"));
        }

        failOnSerialization = true;
    }

    @After
    public void after() {
        failOnSerialization = false;

        factory.shutdownAll();
    }

    @Test
    public void testMapScan() {
        check("SELECT __key, this FROM " + MAP_NAME, 100);
    }

    @Test
    public void testIndexScan() {
        check("SELECT __key, this FROM " + MAP_NAME + " WHERE val = 1", 1);
    }

    private void check(String sql, int expectedCount) {
        try (SqlResult res = member.getSql().execute(sql)) {
            int count = 0;

            for (SqlRow row : res) {
                Object key = row.getObject(0);
                Object value = row.getObject(1);

                assertTrue(key instanceof Key);
                assertTrue(value instanceof Value);

                count++;
            }

            assertEquals(expectedCount, count);
        }
    }

    public static class Key implements Externalizable {

        public int key;

        public Key() {
            // No-op
        }

        private Key(int key) {
            this.key = key;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(key);

            if (failOnSerialization) {
                throw new IOException("Key serialization must not happen.");
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            key = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key that = (Key) o;

            return key == that.key;
        }

        @Override
        public int hashCode() {
            return key;
        }
    }

    public static class Value implements Externalizable {

        public int val;

        public Value() {
            // No-op
        }

        private Value(int val) {
            this.val = val;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);

            if (failOnSerialization) {
                throw new IOException("Value serialization must not happen.");
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            val = in.readInt();
        }
    }
}
