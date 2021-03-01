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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.Value;
import com.hazelcast.query.SampleTestObjects.ValueType;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class QueryIndexTest {

    @RunWith(Parameterized.class)
    public static class ParametrizedQueryIndexTest extends HazelcastTestSupport {
        @Parameter(0)
        public IndexCopyBehavior copyBehavior;

        @Parameters(name = "copyBehavior: {0}")
        public static Collection<Object[]> parameters() {
            return asList(new Object[][]{
                    {IndexCopyBehavior.COPY_ON_READ},
                    {IndexCopyBehavior.COPY_ON_WRITE},
                    {IndexCopyBehavior.NEVER},
            });
        }

        private HazelcastInstance createTestHazelcastInstance() {
            Config config = getConfig();
            config.setProperty(ClusterProperty.INDEX_COPY_BEHAVIOR.getName(), copyBehavior.name());
            return createHazelcastInstance(config);
        }

        @Test
        public void testResultsReturned_whenCustomAttributeIndexed() {
            HazelcastInstance h1 = createTestHazelcastInstance();

            IMap<String, CustomObject> imap = h1.getMap("objects");
            imap.addIndex(IndexType.SORTED, "attribute");

            for (int i = 0; i < 10; i++) {
                CustomAttribute attr = new CustomAttribute(i, 200);
                CustomObject object = new CustomObject("o" + i, randomUUID(), attr);
                imap.put(object.getName(), object);
            }

            EntryObject entry = Predicates.newPredicateBuilder().getEntryObject();
            Predicate predicate = entry.get("attribute").greaterEqual(new CustomAttribute(5, 200));

            Collection<CustomObject> values = imap.values(predicate);
            assertEquals(5, values.size());
        }

        @Test(timeout = 1000 * 60)
        public void testDeletingNonExistingObject() {
            HazelcastInstance instance = createTestHazelcastInstance();
            IMap<Integer, SampleTestObjects.Value> map = instance.getMap(randomMapName());
            map.addIndex(IndexType.HASH, "name");

            map.delete(1);
        }

        @Test(timeout = 1000 * 60)
        public void testInnerIndex() {
            HazelcastInstance instance = createTestHazelcastInstance();
            IMap<String, SampleTestObjects.Value> map = instance.getMap("default");
            map.addIndex(IndexType.HASH, "name");
            map.addIndex(IndexType.HASH, "type.typeName");
            for (int i = 0; i < 10; i++) {
                Value v = new Value("name" + i, i < 5 ? null : new ValueType("type" + i), i);
                map.put("" + i, v);
            }
            Predicate predicate = Predicates.newPredicateBuilder().getEntryObject().get("type.typeName").in("type8", "type6");
            Collection<SampleTestObjects.Value> values = map.values(predicate);
            assertEquals(2, values.size());
            List<String> typeNames = new ArrayList<>();
            for (Value configObject : values) {
                typeNames.add(configObject.getType().getTypeName());
            }
            String[] array = typeNames.toArray(new String[0]);
            Arrays.sort(array);
            assertArrayEquals(typeNames.toString(), new String[]{"type6", "type8"}, array);
        }

        @Test(timeout = 1000 * 60)
        public void testInnerIndexSql() {
            HazelcastInstance instance = createTestHazelcastInstance();
            IMap<String, SampleTestObjects.Value> map = instance.getMap("default");
            map.addIndex(IndexType.HASH, "name");
            map.addIndex(IndexType.HASH, "type.typeName");
            for (int i = 0; i < 4; i++) {
                Value v = new Value("name" + i, new ValueType("type" + i), i);
                map.put("" + i, v);
            }
            Predicate predicate = Predicates.sql("type.typeName='type1'");
            Collection<SampleTestObjects.Value> values = map.values(predicate);
            assertEquals(1, values.size());
            List<String> typeNames = new ArrayList<>();
            for (Value configObject : values) {
                typeNames.add(configObject.getType().getTypeName());
            }
            assertArrayEquals(typeNames.toString(), new String[]{"type1"}, typeNames.toArray(new String[0]));
        }

        @Test(timeout = 1000 * 60)
        public void issue685RemoveIndexesOnClear() {
            HazelcastInstance instance = createTestHazelcastInstance();
            IMap<String, SampleTestObjects.Value> map = instance.getMap("default");
            map.addIndex(IndexType.SORTED, "name");
            for (int i = 0; i < 4; i++) {
                Value v = new Value("name" + i);
                map.put("" + i, v);
            }
            map.clear();
            Predicate predicate = Predicates.sql("name='name0'");
            Collection<SampleTestObjects.Value> values = map.values(predicate);
            assertEquals(0, values.size());
        }

        @Test(timeout = 1000 * 60)
        public void testQueryDoesNotMatchOldResults_whenEntriesAreUpdated() {
            HazelcastInstance instance = createTestHazelcastInstance();
            IMap<String, SampleTestObjects.Value> map = instance.getMap("default");
            map.addIndex(IndexType.SORTED, "name");

            map.put("0", new Value("name"));
            map.put("0", new Value("newName"));

            Collection<SampleTestObjects.Value> values = map.values(Predicates.sql("name='name'"));
            assertEquals(0, values.size());
        }

        @Test(timeout = 1000 * 60)
        public void testOneIndexedFieldsWithTwoCriteriaField() {
            HazelcastInstance h1 = createTestHazelcastInstance();
            IMap<String, Employee> map = h1.getMap("employees");
            map.addIndex(IndexType.HASH, "name");
            map.put("1", new Employee(1L, "joe", 30, true, 100D));
            EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
            PredicateBuilder a = e.get("name").equal("joe");
            Predicate b = e.get("age").equal("30");
            Collection<Employee> actual = map.values(a.and(b));
            assertEquals(1, actual.size());
        }

        @Test(timeout = 1000 * 60)
        public void testPredicateNotEqualWithIndex() {
            HazelcastInstance instance = createTestHazelcastInstance();
            IMap<Integer, Value> map1 = instance.getMap("testPredicateNotEqualWithIndex-ordered");
            IMap<Integer, Value> map2 = instance.getMap("testPredicateNotEqualWithIndex-unordered");
            testPredicateNotEqualWithIndex(map1, true);
            testPredicateNotEqualWithIndex(map2, false);
        }

        private void testPredicateNotEqualWithIndex(IMap<Integer, Value> map, boolean ordered) {
            map.addIndex(ordered ? IndexType.SORTED : IndexType.HASH, "name");
            map.put(1, new Value("abc", 1));
            map.put(2, new Value("xyz", 2));
            map.put(3, new Value("aaa", 3));
            assertEquals(3, map.values(Predicates.sql("name != 'aac'")).size());
            assertEquals(2, map.values(Predicates.sql("index != 2")).size());
            assertEquals(3, map.values(Predicates.sql("name <> 'aac'")).size());
            assertEquals(2, map.values(Predicates.sql("index <> 2")).size());
            assertEquals(3, map.values(Predicates.newPredicateBuilder().getEntryObject().get("name").notEqual("aac")).size());
            assertEquals(2, map.values(Predicates.newPredicateBuilder().getEntryObject().get("index").notEqual(2)).size());
        }
    }

    public static class QueryIndexNoMultipleDeserializationTest extends HazelcastTestSupport {

        @Test
        public void testMultipleIndexesByAttributeDeserialization() {
            final int putCount = 5;

            String mapName = randomMapName();
            Config config = getConfig();
            config.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.BINARY);
            config.getMapConfig(mapName).setCacheDeserializedValues(CacheDeserializedValues.NEVER);
            config.getMetricsConfig().setEnabled(false);

            HazelcastInstance instance = createHazelcastInstance(config);
            IMap<Integer, SerializableObject> map = instance.getMap(mapName);

            map.addIndex(IndexType.HASH, "string");
            map.addIndex(IndexType.SORTED, "string");
            map.addIndex(IndexType.BITMAP, "string");
            for (int idx = 0; idx < putCount; idx++) {
                map.put(idx, new SerializableObject());
            }

            // There was a mention that Predicate API was used
            PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
            Predicate predicate = e.get("string").isNotNull();

            // Since converter obtains for each new index type, we should mix it with put count
            assertEquals(putCount, SerializableObject.deserializationCount);
            SerializableObject.reset();

            Collection<SerializableObject> values = map.values(predicate);
            assertEquals(putCount, values.size());
            assertEquals(putCount, SerializableObject.deserializationCount);
            SerializableObject.reset();
        }

        @Test
        public void testSingleIndexByAttributeDeserialization() {
            final int putCount = 5;
            final int indexCount = 0;

            String mapName = randomMapName();
            Config config = getConfig();
            config.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.BINARY);
            config.getMapConfig(mapName).setCacheDeserializedValues(CacheDeserializedValues.NEVER);
            config.getMetricsConfig().setEnabled(false);

            HazelcastInstance instance = createHazelcastInstance(config);
            IMap<Integer, SerializableObject> map = instance.getMap(mapName);

            map.addIndex(IndexType.HASH, "string");
            map.addIndex(IndexType.HASH, "anotherString");
            for (int idx = 0; idx < putCount; idx++) {
                map.put(idx, new SerializableObject());
            }

            // There was a mention that Predicate API was used
            PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
            Predicate predicate = e.get("string").isNotNull();

            // Since converter obtains for each new index type, we should mix it with put count
            assertEquals(putCount + indexCount, SerializableObject.deserializationCount);
            SerializableObject.reset();

            Collection<SerializableObject> values = map.values(predicate);
            assertEquals(putCount, values.size());
            assertEquals(putCount, SerializableObject.deserializationCount);
            SerializableObject.reset();
        }

        public static class SerializableObject implements DataSerializable {
            static int serializationCount = 0;
            static int deserializationCount = 0;

            static void reset() {
                serializationCount = 0;
                deserializationCount = 0;
            }

            public String string;
            public String anotherString;

            @SuppressWarnings("checkstyle:RedundantModifier")
            public SerializableObject() {
                this.string = randomString();
                this.anotherString = randomString();
            }

            @Override
            public void writeData(ObjectDataOutput out) throws IOException {
                ++serializationCount;
                out.writeString(string);
                out.writeString(anotherString);
            }

            @Override
            public void readData(ObjectDataInput in) throws IOException {
                ++deserializationCount;
                string = in.readString();
                anotherString = in.readString();
            }
        }
    }
}
