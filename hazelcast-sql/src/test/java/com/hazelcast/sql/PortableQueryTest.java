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

import com.google.common.collect.Iterators;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Verifies that the member can extract fields from PortableGenericRecords
 * when it does not have the necessary PortableFactory in its config.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PortableQueryTest extends HazelcastTestSupport {
    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean clusterHasPortableConfig;

    public TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, clusterHasPortableConfig:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false}
        });
    }

    @Before
    public void setup() {
        MapConfig mapConfig = new MapConfig("default");
        mapConfig.setInMemoryFormat(inMemoryFormat);
        Config config = smallInstanceConfig();
        config.addMapConfig(mapConfig);
        if (clusterHasPortableConfig) {
            config.getSerializationConfig().addPortableFactory(1, new TestPortableFactory());
        }
        factory.newHazelcastInstance(config);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testQueryOnPrimitive() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(1, new TestPortableFactory());
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Integer, Object> map = client.getMap("test");
        fillMap(map, 50, ChildPortable::new);

        SqlResult rows = client.getSql().execute("SELECT * FROM test WHERE i >= 45");

        assertEquals(5, Iterators.size(rows.iterator()));
    }

    @Test
    public void testQueryOnObject() {
        //To be able to run comparison methods on objects on the server we need the classes
        assumeTrue(clusterHasPortableConfig);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(1, new TestPortableFactory());
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Integer, ParentPortable> map = client.getMap("test");
        fillMap(map, 100, ParentPortable::new);
        SqlService clientSql = client.getSql();

        ChildPortable expected = new ChildPortable(10);
        SqlResult rows = clientSql.execute("SELECT sa FROM test WHERE c = ?", expected);

        Iterator<SqlRow> iterator = rows.iterator();
        SqlRow row = Iterators.getOnlyElement(iterator);
        String[] rowObject = row.getObject("sa");
        assertEquals(String.valueOf(10), rowObject[0]);
    }

    private <T> void fillMap(IMap<Integer, T> map, int count, Function<Integer, T> constructor) {
        for (int i = 0; i < count; i++) {
            map.put(i, constructor.apply(i));
        }
    }

    static class ChildPortable implements Portable, Comparable<ChildPortable> {
        private int i;
        private int[] ia;

        ChildPortable() {
        }

        ChildPortable(int i) {
            this.i = i;
            this.ia = new int[]{i};
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
            writer.writeInt("i", i);
            writer.writeIntArray("ia", ia);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            i = reader.readInt("i");
            ia = reader.readIntArray("ia");
        }

        @Override
        public int compareTo(ChildPortable o) {
            return i - o.i;
        }
    }

    static class ParentPortable implements Portable, Comparable<ParentPortable> {
        private ChildPortable c;
        private String[] sa;

        ParentPortable() {
        }

        ParentPortable(int i) {
            this.c = new ChildPortable(i);
            this.sa = new String[]{String.valueOf(i)};
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
            writer.writePortable("c", c);
            writer.writeStringArray("sa", sa);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            c = reader.readPortable("c");
            sa = reader.readStringArray("sa");
        }

        @Override
        public int compareTo(ParentPortable o) {
            return c.compareTo(o.c);
        }
    }

    static class TestPortableFactory implements PortableFactory {
        @Override
        public Portable create(int classId) {
            if (classId == 1) {
                return new ChildPortable();
            } else if (classId == 2) {
                return new ParentPortable();
            }
            return null;
        }
    }
}
