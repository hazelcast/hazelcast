/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

/**
 * Verify that maps created on a member joining the cluster, do
 * actually include index & interceptors as expected through
 * execution of PostJoinMapOperation on the joining member.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PostJoinMapOperationTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule
            = new ChangeLoggingRule("log4j2-debug-map.xml");

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testPostJoinMapOperation_mapWithInterceptor() {
        Config config = getConfig();
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);

        // Given: A map with an interceptor on single-node Hazelcast
        HazelcastInstance hz1 = hzFactory.newHazelcastInstance(config);
        IMap<String, Person> map = hz1.getMap("map");
        map.put("foo", new Person("foo", 32));
        map.put("bar", new Person("bar", 35));
        map.addInterceptor(new FixedReturnInterceptor());
        assertEquals(RETURNED_FROM_INTERCEPTOR, map.get("foo"));

        // when: new member joins cluster
        HazelcastInstance hz2 = hzFactory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        // then: values from map reference obtained from node 2 are returned by interceptor
        IMap<String, Person> mapOnNode2 = hz2.getMap("map");
        assertEquals(RETURNED_FROM_INTERCEPTOR, mapOnNode2.get("whatever"));

        // put a value on node 2, then get it and verify it is the one returned by the interceptor
        String keyOwnedByNode2 = generateKeyOwnedBy(hz2);
        map.put(keyOwnedByNode2, new Person("not to be returned", 39));
        assertEquals(RETURNED_FROM_INTERCEPTOR, map.get(keyOwnedByNode2));
    }

    // This test is meant to verify that a query will be executed *with an index* on the joining node
    // See also QueryIndexMigrationTest, which tests that results are as expected.
    @Test
    public void testPostJoinMapOperation_mapWithIndex() {
        Config config = getConfig();
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);

        // given: a map with index on a single-node HazelcastInstance
        HazelcastInstance hz1 = hzFactory.newHazelcastInstance(config);
        IMap<String, Person> map = hz1.getMap("map");
        map.put("foo", new Person("foo", 32));
        map.put("bar", new Person("bar", 70));
        map.addIndex(IndexType.SORTED, "age");

        // when: new node joins and original node is terminated
        HazelcastInstance hz2 = hzFactory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        hzFactory.terminate(hz1);
        waitAllForSafeState(hz2);

        // then: once all migrations are committed, the query is executed *with* the index and
        // returns the expected results.
        IMap<String, Person> mapOnNode2 = hz2.getMap("map");
        AtomicInteger invocationCounter = new AtomicInteger(0);

        // eventually index should be created after join
        assertTrueEventually(() -> {
            Collection<Person> personsWithAgePredicate = mapOnNode2.values(new AgePredicate(invocationCounter));
            assertEquals("index should return 1 match", 1, personsWithAgePredicate.size());
            assertEquals("isIndexed should have located an index", 1, invocationCounter.get());
        });
    }

    @Test
    public void testPostJoinMapOperation_whenMapHasNoData() {
        Config config = getConfig();
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);

        // given: a single node HazelcastInstance with a map configured with index and interceptor
        HazelcastInstance hz1 = hzFactory.newHazelcastInstance(config);
        IMap<String, Person> map = hz1.getMap("map");
        map.addIndex(IndexType.SORTED, "age");
        map.addInterceptor(new FixedReturnInterceptor());

        assertEquals(RETURNED_FROM_INTERCEPTOR, map.get("foo"));

        // when: another member joins the cluster
        HazelcastInstance hz2 = hzFactory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        // then: index & interceptor exist on internal MapContainer on node that joined the cluster
        MapService mapService = getNodeEngineImpl(hz2).getService(MapService.SERVICE_NAME);
        MapContainer mapContainerOnNode2 = mapService.getMapServiceContext().getMapContainer("map");

        assertEquals(1, mapContainerOnNode2.getIndexes().getIndexes().length);
        assertEquals(1, mapContainerOnNode2.getInterceptorRegistry().getInterceptors().size());
        assertEquals(Person.class,
                mapContainerOnNode2.getInterceptorRegistry().getInterceptors().get(0).interceptGet("anything").getClass());
        assertEquals(RETURNED_FROM_INTERCEPTOR.getAge(),
                ((Person) mapContainerOnNode2.getInterceptorRegistry().getInterceptors().get(0).interceptGet("anything")).getAge());

        // also verify via user API
        IMap<String, Person> mapOnNode2 = hz2.getMap("map");
        assertEquals(RETURNED_FROM_INTERCEPTOR, mapOnNode2.get("whatever"));
    }

    private static class Person implements Serializable {
        private final int age;
        private final String name;

        Person(String name, int age) {
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
            return Objects.equals(name, person.name);

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
        private final AtomicInteger isIndexedInvocationCounter;

        public AgePredicate(AtomicInteger atomicInteger) {
            this.isIndexedInvocationCounter = atomicInteger;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index ix = queryContext.getIndex("age");
            if (ix != null) {
                return ix.getRecords(Comparison.GREATER, 50);
            } else {
                return null;
            }
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            Index ix = queryContext.getIndex("age");
            if (ix != null) {
                isIndexedInvocationCounter.incrementAndGet();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            if (mapEntry.getValue() instanceof Person) {
                return ((Person) mapEntry.getValue()).getAge() > 50;
            } else {
                return false;
            }
        }
    }
}
