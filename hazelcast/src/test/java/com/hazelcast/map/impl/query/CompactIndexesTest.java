/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CompactIndexesTest extends HazelcastTestSupport {

    // record cache is off or OBJECT storage is on, 100K records
    // no index: 7s, 3.2GB
    // sequential keys with index: 47s, 4GB, index cost is 0.8GB
    // random keys with index: took forever, extrapolated index cost is 80GB
    // random keys with index + internal index renumbering: 48s, 4GB, index cost is 0.8GB, about 10MB higher

    private IMap<Long, Person> persons;

    @Before
    public void before() {
        HazelcastInstance instance = createHazelcastInstance();
        persons = instance.getMap("persons");
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapConfig mapConfig = config.getMapConfig("persons");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
//        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.NEVER);
        //mapConfig.addMapIndexConfig(new MapIndexConfig("habits[any] -> __key", false));
        return config;
    }

    @Test
    @Ignore("takes really long time to shutdown")
    public void testConsecutiveIds() throws IOException {
        persons.addIndex("habits[any] -> __key", false);

        long start = System.currentTimeMillis();
        for (long i = 0; i < 100000; ++i) {
            long[] habits = new long[4000];
            for (int j = 0; j < habits.length; ++j) {
                habits[j] = (i + j) % 53000;
            }
            persons.put(i, new Person(i, habits));
        }
        System.out.println("Ingestion took " + (System.currentTimeMillis() - start) + "ms");
//        System.in.read();
    }

    @Test
    @Ignore("takes really long time to shutdown")
    public void testRandomIds() throws IOException {
        persons.addIndex("habits[any] -> __key?", false);
        Random random = new Random();

        long start = System.currentTimeMillis();
        for (long i = 0; i < 100000; ++i) {
            long[] habits = new long[4000];
            for (int j = 0; j < habits.length; ++j) {
                habits[j] = (i + j) % 53000;
            }
            long id = random.nextLong() & ~(1L << 63);
            persons.put(id, new Person(id, habits));
        }
        System.out.println("Ingestion took " + (System.currentTimeMillis() - start));
//        System.in.read();
    }

    @Test
    public void testQueries() {
        final int SIZE = 1000;

        persons.addIndex("habits[any] -> __key", false);

        for (long i = 0; i < SIZE; ++i) {
            long[] habits = new long[4000];
            for (int j = 0; j < habits.length; ++j) {
                habits[j] = (i + j) % 53000;
            }
            persons.put(i, new Person(i, habits));
        }

        long start = System.currentTimeMillis();

        assertEquals(11, persons.values(equal("habits[any]", 10)).size());

        assertEquals(6, persons.values(in("habits[any]", 0, 1, 2, 5)).size());

        assertEquals(11, persons.values(or(equal("habits[any]", 10), equal("habits[any]", 5))).size());
        assertEquals(6, persons.values(and(equal("habits[any]", 10), equal("habits[any]", 5))).size());

        assertEquals(SIZE, persons.values(notEqual("habits[any]", -1)).size());
        assertEquals(SIZE - 1, persons.values(notEqual("habits[any]", 0)).size());

        assertEquals(1, persons.values(and(equal("habits[any]", 1), notEqual("habits[any]", 0))).size());
        // this one is slow currently
        //assertEquals(999, persons.values(or(equal("habits[any]", 0), notEqual("habits[any]", 1))).size());

        assertEquals(1,
                persons.values(and(equal("habits[any]", 0), or(equal("habits[any]", 10), equal("habits[any]", 5)))).size());

        System.out.println("Queries took " + (System.currentTimeMillis() - start) + "ms");
    }

    public static class Person implements Serializable {

        public final long id;

        public final long[] habits;

        public Person(long id, long[] habits) {
            this.id = id;
            this.habits = habits;
        }

        @Override
        public String toString() {
            return "Person{id=" + id + "}";
        }

    }

}
