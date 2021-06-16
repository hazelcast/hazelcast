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

package com.hazelcast.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * The aim of this class is to test client server architectures where the server is centralized
 * and has no information about the classes used by the client applications
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientServerUseCaseTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testNestedPortableAsColumn() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getSerializationConfig().addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                if (classId == 1) {
                    return new MainPortable();
                } else if (classId == 2) {
                    return new NestedPortable();
                }
                return null;
            }
        });
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);

        IMap<Object, Object> map = client.getMap("test");

        for (int i = 0; i < 2; i++) {
            map.put(i, new MainPortable(new NestedPortable(i), i));
        }

        SqlResult sqlRows = client.getSql().execute("SELECT code,nested FROM test WHERE code = ? ", 1);

        Iterator<SqlRow> iterator = sqlRows.iterator();
        SqlRow row = iterator.next();
        System.out.println(row);
        assertEquals((Integer) 1, row.getObject(0));
        assertEquals(new NestedPortable(1), row.getObject(1));
        assertFalse(iterator.hasNext());
    }

    public static class NestedPortable implements Portable, Comparable {

        private int id;

        public NestedPortable() {
        }

        public NestedPortable(int id) {
            this.id = id;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("i", id);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            id = reader.readInt("i");
        }

        @Override
        public String toString() {
            return "NestedPortable{"
                    + "id=" + id
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NestedPortable that = (NestedPortable) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public int compareTo(Object o) {
            return id - ((NestedPortable) o).id;
        }
    }


    public static class MainPortable implements Portable {

        private int code;
        private NestedPortable nestedPortable;

        public MainPortable(NestedPortable nestedPortable, int code) {
            this.nestedPortable = nestedPortable;
            this.code = code;
        }

        public MainPortable() {
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortable("nested", nestedPortable);
            writer.writeInt("code", code);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            nestedPortable = reader.readPortable("nested");
            code = reader.readInt("code");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MainPortable that = (MainPortable) o;
            return code == that.code && Objects.equals(nestedPortable, that.nestedPortable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, nestedPortable);
        }
    }
}
