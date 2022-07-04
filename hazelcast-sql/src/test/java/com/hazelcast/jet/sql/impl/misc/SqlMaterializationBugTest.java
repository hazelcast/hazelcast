/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Collections.singletonList;

/**
 * Tests for the issue reported in https://github.com/hazelcast/hazelcast/issues/17554
 */
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlMaterializationBugTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        createMapping("map", int.class, int.class);
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 2);
    }

    @Test
    public void test_0() {
        assertRowsOrdered(
                "SELECT * FROM (SELECT * FROM map WHERE 0 = 0) WHERE 1 = 1",
                singletonList(new Row(1, 2))
        );
    }

    @Test
    public void test_1() {
        assertRowsOrdered(
                "SELECT * FROM (SELECT * FROM map WHERE 1 = 1) WHERE 1 = 1",
                singletonList(new Row(1, 2))
        );
    }
}
