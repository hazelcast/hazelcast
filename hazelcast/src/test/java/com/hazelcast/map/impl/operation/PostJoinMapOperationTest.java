/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.impl.ComparisonType;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * Verify that maps created on a member joining the cluster, do actually include index & interceptors
 * as expected through execution of PostJoinMapOperation on the joining member.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PostJoinMapOperationTest extends HazelcastTestSupport {

    public final AtomicInteger isIndexedInvoked = new AtomicInteger(0);

    private static class Person implements Serializable {
        private final int age;
        private final String name;

        public Person(String name, int age) {
            this.age = age;
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Person)) {
                return false;
            }

            Person person = (Person) o;

            if (age != person.age) {
                return false;
            }
            return name != null ? name.equals(person.name) : person.name == null;

        }

        @Override
        public int hashCode() {
            int result = age;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }

    private static final Person RETURNED_FROM_INTERCEPTOR = new Person("THE_PERSON", 100);

    public static class FixedReturnInterceptor implements MapInterceptor {

        @Override
        public Object interceptGet(Object value) {
            return RETURNED_FROM_INTERCEPTOR;
        }

        @Override
        public void afterGet(Object value) {

        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            // allow put operations to proceed
            return null;
        }

        @Override
        public void afterPut(Object value) {

        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return RETURNED_FROM_INTERCEPTOR;
        }

        @Override
        public void afterRemove(Object value) {

        }
    }

    // if it locates an index on Person.age, increments isIndexedInvoked
    public class AgePredicate implements IndexAwarePredicate {
        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index ix = queryContext.getIndex("age");
            if (ix != null) {
                return ix.getSubRecords(ComparisonType.GREATER, 50);
            }
            else {
                return null;
            }
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            Index ix = queryContext.getIndex("age");
            if (ix != null) {
                isIndexedInvoked.incrementAndGet();
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            if (mapEntry.getValue() instanceof Person) {
                return ((Person)mapEntry.getValue()).getAge() > 50;
            }
            else {
                return false;
            }
        }
    }

    @Test
    public void testPostJoinMapOperation_mapWithInterceptor() {
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance hz1 = hzFactory.newHazelcastInstance();
        IMap<String, Person> map = hz1.getMap("map");
        map.put("foo", new Person("foo", 32));
        map.put("bar", new Person("bar", 35));
        map.addInterceptor(new FixedReturnInterceptor());

        assertEquals(RETURNED_FROM_INTERCEPTOR, map.get("foo"));

        // new member joins cluster
        HazelcastInstance hz2 = hzFactory.newHazelcastInstance();
        waitAllForSafeState(hz1, hz2);

        IMap<String, Person> mapOnNode2 = hz2.getMap("map");
        assertEquals(RETURNED_FROM_INTERCEPTOR, mapOnNode2.get("whatever"));

        // put a value on node 2, then get it and verify it is the one returned by the interceptor
        String keyOwnedByNode2 = generateKeyOwnedBy(hz2);
        map.put(keyOwnedByNode2, new Person("not to be returned", 39));
        assertEquals(RETURNED_FROM_INTERCEPTOR, map.get(keyOwnedByNode2));

        hzFactory.terminateAll();
    }

    @Test
    public void testPostJoinMapOperation_mapWithIndex() {
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance hz1 = hzFactory.newHazelcastInstance();
        IMap<String, Person> map = hz1.getMap("map");
        map.put("foo", new Person("foo", 32));
        map.put("bar", new Person("bar", 70));
        map.addIndex("age", true);

        HazelcastInstance hz2 = hzFactory.newHazelcastInstance();
        waitAllForSafeState(hz1, hz2);

        hzFactory.terminate(hz1);
        waitAllForSafeState(hz1, hz2);

        IMap<String, Person> mapOnNode2 = hz2.getMap("map");
        Collection<Person> personsWithAgePredicate = mapOnNode2.values(new AgePredicate());
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return isIndexedInvoked.get();
            }
        }, 1);
        assertEquals(1, personsWithAgePredicate.size());

        hzFactory.terminate(hz2);
    }

    @Test
    public void testPostJoinMapOperation_whenMapHasNoData() {
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance hz1 = hzFactory.newHazelcastInstance();
        IMap<String, Person> map = hz1.getMap("map");
        map.addIndex("age", true);
        map.addInterceptor(new FixedReturnInterceptor());

        assertEquals(RETURNED_FROM_INTERCEPTOR, map.get("foo"));

        HazelcastInstance hz2 = hzFactory.newHazelcastInstance();
        waitAllForSafeState(hz1, hz2);
        MapService mapService = getNodeEngineImpl(hz2).getService(MapService.SERVICE_NAME);
        // verify index & interceptor exist on internal MapContainer
        MapContainer mapContainerOnNode2 = mapService.getMapServiceContext().getMapContainer("map");
        assertEquals(1,  mapContainerOnNode2.getIndexes().getIndexes().length);
        assertEquals(1,  mapContainerOnNode2.getInterceptorRegistry().getInterceptors().size());
        assertEquals(Person.class,
                mapContainerOnNode2.getInterceptorRegistry().getInterceptors().get(0).interceptGet("anything").getClass());
        assertEquals(RETURNED_FROM_INTERCEPTOR.getAge(),
                ((Person)mapContainerOnNode2.getInterceptorRegistry().getInterceptors().get(0).interceptGet("anything")).getAge());

        // also verify via user API
        IMap<String, Person> mapOnNode2 = hz2.getMap("map");
        assertEquals(RETURNED_FROM_INTERCEPTOR, mapOnNode2.get("whatever"));

        hzFactory.terminateAll();
    }

}
