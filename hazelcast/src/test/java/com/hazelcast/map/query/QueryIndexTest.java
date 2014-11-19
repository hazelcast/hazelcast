/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.SampleObjects.Value;
import com.hazelcast.query.SampleObjects.ValueType;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.UUID.randomUUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueryIndexTest extends HazelcastTestSupport {

    @Test
    public void testResultsReturned_whenCustomAttributeIndexed() throws Exception {

        HazelcastInstance h1 = createHazelcastInstance();

        IMap<String, CustomObject> imap = h1.getMap("objects");
        imap.addIndex("attribute", true);

        for (int i = 0; i < 10; i++) {
            CustomAttribute attr = new CustomAttribute(i, 200);
            CustomObject object = new CustomObject("o" + i, randomUUID(), attr);
            imap.put(object.getName(), object);
        }

        EntryObject entry = new PredicateBuilder().getEntryObject();
        Predicate predicate = entry.get("attribute").greaterEqual(new CustomAttribute(5, 200));

        Collection<CustomObject> values = imap.values(predicate);
        assertEquals(5, values.size());
    }

    @Test(timeout = 1000 * 60)
    public void testInnerIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("name", false);
        map.addIndex("type.typeName", false);
        for (int i = 0; i < 10; i++) {
            Value v = new Value("name" + i, i < 5 ? null : new ValueType("type" + i), i);
            map.put("" + i, v);
        }
        Predicate predicate = new PredicateBuilder().getEntryObject().get("type.typeName").in("type8", "type6");
        Collection<SampleObjects.Value> values = map.values(predicate);
        assertEquals(2, values.size());
        List<String> typeNames = new ArrayList<String>();
        for (Value configObject : values) {
            typeNames.add(configObject.getType().getTypeName());
        }
        String[] array = typeNames.toArray(new String[0]);
        Arrays.sort(array);
        assertArrayEquals(typeNames.toString(), new String[]{"type6", "type8"}, array);
    }

    @Test(timeout = 1000 * 60)
    public void testInnerIndexSql() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("name", false);
        map.addIndex("type.typeName", false);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i, new ValueType("type" + i), i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("type.typeName='type1'");
        Collection<SampleObjects.Value> values = map.values(predicate);
        assertEquals(1, values.size());
        List<String> typeNames = new ArrayList<String>();
        for (Value configObject : values) {
            typeNames.add(configObject.getType().getTypeName());
        }
        assertArrayEquals(typeNames.toString(), new String[]{"type1"}, typeNames.toArray(new String[0]));
    }

    @Test(timeout = 1000 * 60)
    public void issue685RemoveIndexesOnClear() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        map.clear();
        Predicate predicate = new SqlPredicate("name='name0'");
        Collection<SampleObjects.Value> values = map.values(predicate);
        assertEquals(0, values.size());
    }

    @Test(timeout = 1000 * 60)
        public void testOneIndexedFieldsWithTwoCriteriaField() throws Exception {
            HazelcastInstance h1 = createHazelcastInstance();
            IMap imap = h1.getMap("employees");
            imap.addIndex("name", false);
    //        imap.addIndex("age", false);
            imap.put("1", new Employee(1L, "joe", 30, true, 100D));
            EntryObject e = new PredicateBuilder().getEntryObject();
            PredicateBuilder a = e.get("name").equal("joe");
            Predicate b = e.get("age").equal("30");
            Collection<Object> actual = imap.values(a.and(b));
            assertEquals(1, actual.size());
        }

    @Test(timeout = 1000 * 60)
    public void testPredicateNotEqualWithIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map1 = instance.getMap("testPredicateNotEqualWithIndex-ordered");
        IMap map2 = instance.getMap("testPredicateNotEqualWithIndex-unordered");
        testPredicateNotEqualWithIndex(map1, true);
        testPredicateNotEqualWithIndex(map2, false);
    }

    private void testPredicateNotEqualWithIndex(IMap map, boolean ordered) {
        map.addIndex("name", ordered);
        map.put(1, new Value("abc", 1));
        map.put(2, new Value("xyz", 2));
        map.put(3, new Value("aaa", 3));
        assertEquals(3, map.values(new SqlPredicate("name != 'aac'")).size());
        assertEquals(2, map.values(new SqlPredicate("index != 2")).size());
        assertEquals(3, map.values(new PredicateBuilder().getEntryObject().get("name").notEqual("aac")).size());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("index").notEqual(2)).size());
    }


}
