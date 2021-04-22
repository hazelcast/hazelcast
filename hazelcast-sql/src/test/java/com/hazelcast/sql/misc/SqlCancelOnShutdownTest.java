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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Test that ensures that queries are cancelled properly in the case of member shutdown.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlCancelOnShutdownTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final int KEY_COUNT = 100_000;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance instance;

    @Before
    public void before() {
        instance = factory.newHazelcastInstance(smallInstanceConfig());

        IMap<Integer, Integer> map = instance.getMap(MAP_NAME);
        Map<Integer, Integer> localMap = new HashMap<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            localMap.put(i, i);
        }

        map.putAll(localMap);
    }

    @After
    public void after() {
        factory.shutdownAll();

        instance = null;
    }

    @Test
    public void testShutdown_initiator() {
        QueryStateRegistry stateRegistry = sqlInternalService(instance).getStateRegistry();

        try (SqlResult result = instance.getSql().execute("SELECT * FROM " + MAP_NAME)) {
            try {
                boolean first = true;

                for (SqlRow ignore : result) {
                    if (first) {
                        assertStateCount(stateRegistry, 1);

                        instance.shutdown();

                        first = false;
                    }
                }

                fail("Must fail");
            } catch (HazelcastSqlException e) {
                assertEquals(SqlErrorCode.GENERIC, e.getCode());
                assertEquals("SQL query has been cancelled due to member shutdown", e.getMessage());

                assertStateCount(stateRegistry, 0);
            }
        }
    }

    private static void assertStateCount(QueryStateRegistry stateRegistry, int expectedCount) {
        assertTrueEventually(() -> assertEquals(expectedCount, stateRegistry.getStates().size()));
    }
}
