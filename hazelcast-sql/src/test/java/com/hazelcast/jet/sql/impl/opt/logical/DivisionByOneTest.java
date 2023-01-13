/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({QuickTest.class, ParallelJVMTest.class})
public class DivisionByOneTest extends SqlTestSupport {
    private static final String MAP_NAME = "test";

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void test_whenDividingByOneInFloor_then_properResultsAreReturned() {
        createMapping(MAP_NAME, Double.class, Double.class);
        instance().getMap(MAP_NAME).put(2.0, 2.0);
        String sql = "SELECT floor(this / 1) FROM " + MAP_NAME;
        assertRowsAnyOrder(sql, rows(1, 2.0));
    }
}
