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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CreateViewStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_createViewStatementBase() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        createMapping("map", Integer.class, Integer.class);

        String sql = "CREATE VIEW v AS SELECT * FROM map";
        assertThatThrownBy(() -> instance().getSql().execute(sql));
    }

    @Test
    public void test_dropViewStatementBase() {
        String sql = "DROP VIEW v";
        assertThatThrownBy(() -> instance().getSql().execute(sql));
    }
}
