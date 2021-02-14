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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientCursorCleanupTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private static volatile boolean fail;

    private final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

    private HazelcastInstance member;
    private HazelcastInstance client;

    @Before
    public void before() {
        member = factory.newHazelcastInstance(smallInstanceConfig());
        client = factory.newHazelcastClient(new ClientConfig());

        member.getConfig().addMapConfig(new MapConfig().setName(MAP_NAME).setInMemoryFormat(InMemoryFormat.OBJECT));
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
        IMap<Integer, Person> map = member.getMap(MAP_NAME);
        map.put(0, new Person());
        map.put(1, new Person());

        fail = true;

        try {
            SqlResult result = client.getSql().execute(statement());

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
        IMap<Integer, Person> map = member.getMap(MAP_NAME);
        map.put(0, new Person());
        map.put(1, new Person());
        map.put(2, new Person());

        try {
            SqlResult result = client.getSql().execute(statement());

            for (SqlRow ignore : result) {
                fail = true;
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

    private static SqlStatement statement() {
        return new SqlStatement("SELECT * FROM " + MAP_NAME).setCursorBufferSize(1);
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
        public void readData(ObjectDataInput in) {
            // No-op
        }
    }
}
