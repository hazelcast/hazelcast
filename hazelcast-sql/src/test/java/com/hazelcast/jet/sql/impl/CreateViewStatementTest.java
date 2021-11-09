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
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CreateViewStatementTest extends SqlTestSupport {
    private IMap<Integer, Integer> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        createMapping("map", Integer.class, Integer.class);

        map = instance().getMap("map");
        map.put(1, 10);
    }

    @Test
    public void createViewStatementBaseTest() {
        String sql = "CREATE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, View> viewStorage = instance().getReplicatedMap("__sql.views");
        assertThat(viewStorage.containsKey("v")).isTrue();
        assertThat(viewStorage.get("v").query()).isEqualTo("SELECT \"map\".\"__key\", \"map\".\"this\"\n" +
                "FROM \"hazelcast\".\"public\".\"map\" AS \"map\"");

        List<Row> expected = new ArrayList<>();
        expected.add(new Row(1, 10));
        assertRowsAnyOrder(viewStorage.get("v").query(), expected);
    }

    @Test
    public void dropViewStatementBaseTest() {
        String sql = "CREATE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, View> viewStorage = instance().getReplicatedMap("__sql.views");
        assertThat(viewStorage.containsKey("v")).isTrue();

        sql = "DROP VIEW v";
        instance().getSql().execute(sql);
        assertThat(viewStorage.containsKey("v")).isFalse();
    }
}
