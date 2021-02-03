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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlTestInstanceFactory;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.client.SqlClientService;
import com.hazelcast.sql.impl.client.SqlClientUtils;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("StatementWithEmptyBody")
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlNoDeserializationTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final int KEY_COUNT = 100;

    private static final String SQL = "SELECT __key, this FROM " + MAP_NAME;

    private static final String ERROR_KEY = "KEY FAILURE";
    private static final String ERROR_VALUE = "VALUE FAILURE";

    private final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

    private HazelcastInstance member1;
    private HazelcastInstance member2;
    private HazelcastInstance client;

    @Before
    public void before() {
        member1 = factory.newHazelcastInstance(config());
        member2 = factory.newHazelcastInstance(config());
        client = factory.newHazelcastClient(new ClientConfig());

        prepare();
    }

    @After
    public void after() {
        factory.shutdownAll();

        member1 = null;
        member2 = null;
        client = null;
    }

    private static Config config() {
        Config config = smallInstanceConfig();

        config.addMapConfig(new MapConfig(MAP_NAME).setInMemoryFormat(InMemoryFormat.BINARY).setBackupCount(0));

        return config;
    }

    @Test
    public void testMember() {
        try (SqlResult res = member1.getSql().execute(SQL)) {
            for (SqlRow row : res) {
                SqlRowImpl row0 = (SqlRowImpl) row;

                row0.getObjectRaw(0);
                row0.getObjectRaw(1);

                checkFailure(row, true);
                checkFailure(row, false);
            }
        }
    }

    @Test
    public void testClient() {
        int pageSize = KEY_COUNT / 2;

        SqlClientService clientService = (SqlClientService) client.getSql();

        Connection connection = clientService.getRandomConnection();

        // Get the first page through the "execute" request
        QueryId queryId = QueryId.create(UUID.randomUUID());

        ClientMessage executeRequest = SqlExecuteCodec.encodeRequest(
            SQL,
            Collections.emptyList(),
            Long.MAX_VALUE,
            pageSize,
            null,
            SqlClientUtils.expectedResultTypeToByte(SqlExpectedResultType.ROWS),
            queryId

        );

        SqlExecuteCodec.ResponseParameters executeResponse = SqlExecuteCodec.decodeResponse(
            clientService.invokeOnConnection(connection, executeRequest)
        );

        if (executeResponse.error != null) {
            fail(executeResponse.error.getMessage());
        }

        assertNotNull(executeResponse.rowPage);
        assertEquals(pageSize, executeResponse.rowPage.getRowCount());

        // Get the second page through the "execute" request
        ClientMessage fetchRequest = SqlFetchCodec.encodeRequest(
            queryId,
            pageSize
        );

        SqlFetchCodec.ResponseParameters fetchResponse = SqlFetchCodec.decodeResponse(
            clientService.invokeOnConnection(connection, fetchRequest)
        );

        if (fetchResponse.error != null) {
            fail(fetchResponse.error.getMessage());
        }

        assertNotNull(fetchResponse.rowPage);
        assertEquals(pageSize, fetchResponse.rowPage.getRowCount());
    }

    private void checkFailure(SqlRow row, boolean key) {
        int index = key ? 0 : 1;
        String expectedMessage = key ? ERROR_KEY : ERROR_VALUE;

        try {
            row.getObject(index);

            fail();
        } catch (HazelcastSqlException e) {
            assertEquals(SqlErrorCode.DATA_EXCEPTION, e.getCode());
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @SuppressWarnings("EmptyTryBlock")
    private void prepare() {
        populate(false);

        // Make sure that we cached the plan to avoid failures on automatic schema inference.
        try (SqlResult ignore = member1.getSql().execute(SQL)) {
            // No-op
        }

        try (SqlResult ignore = member2.getSql().execute(SQL)) {
            // No-op
        }

        populate(true);
    }

    private void populate(boolean fail) {
        Map<PersonKey, Person> localMap = new HashMap<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            localMap.put(new PersonKey(i, fail), new Person(fail));
        }

        IMap<PersonKey, Person> map = member1.getMap(MAP_NAME);

        map.putAll(localMap);

        if (fail) {
            for (int i = 0; i < KEY_COUNT; i++) {
                map.remove(new PersonKey(i, false));
            }
        }

        assertEquals(KEY_COUNT, map.size());
    }

    public static class PersonKey implements DataSerializable {

        private int id;
        private boolean fail;

        public PersonKey() {
            // No-op
        }

        public PersonKey(int id, boolean fail) {
            this.id = id;
            this.fail = fail;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(id);
            out.writeBoolean(fail);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readInt();
            fail = in.readBoolean();

            if (fail) {
                throw new IOException(ERROR_KEY);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PersonKey personKey = (PersonKey) o;

            if (id != personKey.id) {
                return false;
            }

            return fail == personKey.fail;
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + (fail ? 1 : 0);
            return result;
        }
    }

    public static class Person implements DataSerializable {

        private boolean fail;

        public Person() {
            // No-op
        }

        public Person(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(fail);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            fail = in.readBoolean();

            if (fail) {
                throw new IOException(ERROR_VALUE);
            }
        }
    }
}
