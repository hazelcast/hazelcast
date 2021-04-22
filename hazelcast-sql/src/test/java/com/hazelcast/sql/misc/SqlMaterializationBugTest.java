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

package com.hazelcast.sql.misc;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the issue reported in https://github.com/hazelcast/hazelcast/issues/17554
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlMaterializationBugTest extends SqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
    private HazelcastInstance instance;

    @Before
    public void before() {
        instance = factory.newHazelcastInstance();

        IMap<Integer, Integer> map = instance.getMap("map");
        map.put(1, 2);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void test_0() {
        List<SqlRow> rows = execute(instance, "SELECT * FROM (SELECT * FROM map WHERE 0 = 0) WHERE 1 = 1");

        assertEquals(1, rows.size());
        assertEquals(Integer.valueOf(1), rows.get(0).getObject(0));
        assertEquals(Integer.valueOf(2), rows.get(0).getObject(1));
    }

    @Test
    public void test_1() {
        List<SqlRow> rows = execute(instance, "SELECT * FROM (SELECT * FROM map WHERE 1 = 1) WHERE 1 = 1");

        assertEquals(1, rows.size());
        assertEquals(Integer.valueOf(1), rows.get(0).getObject(0));
        assertEquals(Integer.valueOf(2), rows.get(0).getObject(1));
    }
}
