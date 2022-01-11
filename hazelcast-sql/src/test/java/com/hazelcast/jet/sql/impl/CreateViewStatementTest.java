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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CreateViewStatementTest extends SqlTestSupport {
    private static final String LE = System.lineSeparator();
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
    public void when_createsView_then_succeeds() {
        String sql = "CREATE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, Object> viewStorage = instance().getReplicatedMap("__sql.catalog");
        assertThat(viewStorage.containsKey("v")).isTrue();
        assertThat(viewStorage.get("v")).isInstanceOf(View.class);
        assertThat(((View) viewStorage.get("v")).query()).isEqualTo("SELECT \"map\".\"__key\", \"map\".\"this\"" + LE
                + "FROM \"hazelcast\".\"public\".\"map\" AS \"map\"");


        assertRowsAnyOrder(((View) viewStorage.get("v")).query(), Collections.singletonList(new Row(1, 10)));
    }

    @Test
    public void test_unnecessaryOrReplaceOption() {
        String sql = "CREATE OR REPLACE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, Object> viewStorage = instance().getReplicatedMap("__sql.catalog");
        assertThat(viewStorage.containsKey("v")).isTrue();
        assertThat(viewStorage.get("v")).isInstanceOf(View.class);
        assertThat(((View) viewStorage.get("v")).query()).isEqualTo("SELECT \"map\".\"__key\", \"map\".\"this\"" + LE
                + "FROM \"hazelcast\".\"public\".\"map\" AS \"map\"");


        assertRowsAnyOrder(((View) viewStorage.get("v")).query(), Collections.singletonList(new Row(1, 10)));
    }

    @Test
    public void when_createsViewWithReplace_then_succeeds() {
        String sql = "CREATE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, Object> viewStorage = instance().getReplicatedMap("__sql.catalog");
        assertThat(viewStorage.get("v")).isInstanceOf(View.class);
        assertThat(((View) viewStorage.get("v")).query()).isEqualTo("SELECT \"map\".\"__key\", \"map\".\"this\"" + LE
                + "FROM \"hazelcast\".\"public\".\"map\" AS \"map\"");

        createMapping("map2", Integer.class, Integer.class);

        sql = "CREATE OR REPLACE VIEW v AS SELECT * FROM map2";
        instance().getSql().execute(sql);

        assertThat(viewStorage.get("v")).isInstanceOf(View.class);
        assertThat(((View) viewStorage.get("v")).query()).isEqualTo("SELECT \"map2\".\"__key\", \"map2\".\"this\"" + LE
                + "FROM \"hazelcast\".\"public\".\"map2\" AS \"map2\"");
    }

    @Test
    public void when_createsViewWithExistingName_then_throws() {
        String sql = "CREATE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, Object> viewStorage = instance().getReplicatedMap("__sql.catalog");
        assertThat(viewStorage.containsKey("v")).isTrue();
        assertThat(viewStorage.get("v")).isInstanceOf(View.class);

        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("Mapping or view already exists: v");
    }

    @Test
    public void when_replaceViewWithIfNotExists_then_throws() {
        String sql = "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT * FROM map";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("OR REPLACE in conjunction with IF NOT EXISTS not supported");
    }

    @Test
    public void when_createsViewWithExistingMappingWithEqualName_then_throws() {
        createMapping("v", Integer.class, Integer.class);

        String sql = "CREATE VIEW v AS SELECT * FROM map";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("Mapping or view already exists: v");
    }

    @Test
    public void when_incorrectQuery_then_fails() {
        assertThatThrownBy(() -> instance().getSql().execute("CREATE VIEW v AS SELECT -"))
                .hasMessageContaining("Encountered \"<EOF>\" at line 1");
    }

    @Test
    public void when_dropView_then_succeeds() {
        String sql = "CREATE VIEW v AS SELECT * FROM map";
        instance().getSql().execute(sql);

        ReplicatedMap<String, Object> viewStorage = instance().getReplicatedMap("__sql.catalog");
        assertThat(viewStorage.containsKey("v")).isTrue();

        sql = "DROP VIEW v";
        instance().getSql().execute(sql);
        assertThat(viewStorage.containsKey("v")).isFalse();
    }

    @Test
    public void when_dropAbsentViewWithIfExists_then_throws() {
        String sql = "DROP VIEW v";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("View does not exist: v");
    }
}
