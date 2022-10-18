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

package com.hazelcast.jet.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlCatalogInitTest extends SqlTestSupport {
    public static final String MAP_NAME = randomName();
    public static final Config CONFIG = smallInstanceConfig();

    // test case for https://github.com/hazelcast/hazelcast/issues/21632
    @Test
    public void test() {
        HazelcastInstance instance1 = createHazelcastInstance(CONFIG);
        createHazelcastInstance(CONFIG);
        createMapping(instance1, MAP_NAME, Integer.class, Integer.class);

        HazelcastInstance instance3 = createHazelcastInstance(CONFIG);
        assertClusterSizeEventually(3, instance3);
        waitAllForSafeState(instance3);

        // There is no guarantee that even if partitions are in safe state the replicated map is migrated. If initial replication
        // fails (for example due to stressed CPU) then it will be replicated with anti entropy mechanism which takes 30
        // seconds. In 5.2 we switched to IMap. I see no way o guarantee that __sql.catalog is available one the new member
        // soon after the start. To make that test not failing let's just wait for the replication.
        ReplicatedMap<String, Object> sqlCatalog = instance3.getReplicatedMap("__sql.catalog");
        assertTrueEventually(() -> {
            assertTrue(sqlCatalog.size() > 0);
        });

        SqlResult result = instance3.getSql().execute("select * from " + MAP_NAME);
        assertFalse(result.iterator().hasNext());
    }
}
