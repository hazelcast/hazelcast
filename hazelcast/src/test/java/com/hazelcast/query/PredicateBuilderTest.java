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

package com.hazelcast.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PredicateBuilderTest extends HazelcastTestSupport {


    @Test
    public void get_keyAttribute() {
        HazelcastInstance hz = createHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.key().get("id").equal("10").and(e.get("name").equal("value1"));

        IMap<Id, Value> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(new Id("10"), new Value("value1"));
        hazelcastLookupMap.put(new Id("20"), new Value("value2"));
        hazelcastLookupMap.put(new Id("30"), new Value("value3"));

        Collection<Value> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(new Value("value1")));
    }

    @Test
    public void get_key() {
         HazelcastInstance hz = createHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.key().equal(10L);

        IMap<Integer, Integer> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(10, 1);
        hazelcastLookupMap.put(30, 2);

        Collection<Integer> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(1));
    }

    @Test
    public void get_this() {
         HazelcastInstance hz = createHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.get("this").equal(1L);

        IMap<Integer, Integer> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(10, 1);
        hazelcastLookupMap.put(30, 2);

        Collection<Integer> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(1));
    }

    @Test
    public void get_attribute() {
       HazelcastInstance hz = createHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.get("id").equal("10");

        IMap<Integer, Id> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(1,new Id("10"));
        hazelcastLookupMap.put(2,new Id("20"));
        hazelcastLookupMap.put(3,new Id("30"));

        Collection<Id> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(new Id("10")));
    }

    private static class Id implements Serializable {
        private String id;

        public Id(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Id id1 = (Id) o;

            if (id != null ? !id.equals(id1.id) : id1.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }

    private static class Value implements Serializable {
        private String name;

        private Value(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Value value = (Value) o;

            if (name != null ? !name.equals(value.name) : value.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }
}
