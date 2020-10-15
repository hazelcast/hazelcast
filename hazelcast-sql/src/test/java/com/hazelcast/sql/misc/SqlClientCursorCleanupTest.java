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

package com.hazelcast.sql.misc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlTestInstanceFactory;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("StatementWithEmptyBody")
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientCursorCleanupTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final int KEY_COUNT = 10000;

    private static volatile boolean fail;

    private final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

    private HazelcastInstance member;
    private HazelcastInstance client;

    @Before
    public void before() {
        member = factory.newHazelcastInstance(smallInstanceConfig());
        client = factory.newHazelcastClient(new ClientConfig());

        IMap<Long, Person> map = member.getMap(MAP_NAME);
        Map<Long, Person> localMap = new HashMap<>();

        for (long i = 0; i < KEY_COUNT; i++) {
            localMap.put(i, new Person());
        }

        map.putAll(localMap);
    }

    @After
    public void after() {
        fail = false;

        factory.shutdownAll();

        member = null;
        client = null;
    }

    @Test
    public void testExceptionOnExecute() {
        fail = true;

        try {
            SqlResult result = client.getSql().execute("SELECT * FROM " + MAP_NAME);

            for (SqlRow ignore : result) {
                // No-op.
            }

            fail("Must fail");
        } catch (Exception e) {
            assertNoState();
        }
    }

    @Test
    public void testExceptionOnFetch() {
        try {
            SqlResult result = client.getSql().execute("SELECT * FROM " + MAP_NAME);

            fail = true;

            for (SqlRow ignore : result) {
                // No-op.
            }

            fail("Must fail");
        } catch (Exception e) {
            assertNoState();
        }
    }

    private void assertNoState() {
        SqlInternalService service = ((SqlServiceImpl) member.getSql()).getInternalService();

        assertEquals(0, service.getStateRegistry().getStates().size());
        assertEquals(0, service.getClientStateRegistry().getCursorCount());
    }

    public static class Person implements DataSerializable {

        public Person() {
            // No-op
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            if (fail) {
                throw new IOException();
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // No-op
        }
    }
}
