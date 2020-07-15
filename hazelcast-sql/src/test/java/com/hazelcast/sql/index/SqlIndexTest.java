/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.index;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private static final TestHazelcastFactory FACTORY = new TestHazelcastFactory(2);
    private static HazelcastInstance member;
    private static IMap<Long, Person> map;

    @BeforeClass
    public static void beforeClass() throws Exception {
        member = FACTORY.newHazelcastInstance();
        // FACTORY.newHazelcastInstance();

        map = member.getMap(MAP_NAME);
        map.addIndex(IndexType.SORTED, "age");
        Thread.sleep(1000);

        map.put(1L, new Person(30));
        map.put(2L, new Person(40));
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void testIndex() {
        List<SqlRow> rows = execute(member, "SELECT age FROM map WHERE age > 35");

        assertEquals(1, rows.size());
        assertEquals((Integer) 40, rows.get(0).getObject(0));
    }

    public static class Person implements Serializable {
        public int age;

        public Person(int age) {
            this.age = age;
        }
    }
}
