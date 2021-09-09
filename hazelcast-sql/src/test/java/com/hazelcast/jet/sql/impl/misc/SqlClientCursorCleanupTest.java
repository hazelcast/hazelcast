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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SqlClientCursorCleanupTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private static volatile boolean fail;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

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

        assertEquals(0, service.getResultRegistry().getResultCount());
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
