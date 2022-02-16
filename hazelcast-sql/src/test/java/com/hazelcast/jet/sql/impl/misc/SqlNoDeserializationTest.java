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
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.client.SqlClientService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlNoDeserializationTest extends SqlTestSupport {

    protected static final int PORTABLE_FACTORY_ID = 1;
    private static final int PORTABLE_KEY_ID = 1;
    private static final int PORTABLE_VALUE_ID = 2;

    private static final String MAP_NAME = "map";
    private static final int KEY_COUNT = 100;

    private static final String SQL = "SELECT __key, this FROM " + MAP_NAME;

    private static final String ERROR_KEY = "KEY FAILURE";
    private static final String ERROR_VALUE = "VALUE FAILURE";


    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(1, config(), clientConfig());
    }

    @Before
    public void before() {
        prepare();
    }

    private static Config config() {
        Config config = smallInstanceConfig();

        config.addMapConfig(new MapConfig(MAP_NAME).setInMemoryFormat(InMemoryFormat.BINARY));
        config.getSerializationConfig().addPortableFactory(PORTABLE_FACTORY_ID, portableFactory());

        return config;
    }

    private static ClientConfig clientConfig() {
        ClientConfig config = new ClientConfig();

        config.getSerializationConfig().addPortableFactory(PORTABLE_FACTORY_ID, portableFactory());

        return config;
    }

    protected static PortableFactory portableFactory() {
        return classId -> {
            if (classId == PORTABLE_KEY_ID) {
                return new PersonKey();
            } else {
                assertEquals(classId, PORTABLE_VALUE_ID);

                return new Person();
            }
        };
    }

    @Test
    public void testMember() {
        try (SqlResult res = instance().getSql().execute(SQL)) {
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

        SqlClientService clientService = (SqlClientService) client().getSql();

        ClientConnection connection = clientService.getQueryConnection();

        // Get the first page through the "execute" request
        QueryId queryId = QueryId.create(connection.getRemoteUuid());

        ClientMessage executeRequest = SqlExecuteCodec.encodeRequest(
                SQL,
                Collections.emptyList(),
                Long.MAX_VALUE,
                pageSize,
                null,
                SqlExpectedResultType.ROWS.getId(),
                queryId,
                false
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
        } catch (HazelcastSerializationException e) {
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    private void prepare() {
        Map<PersonKey, Person> localMap = new HashMap<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            localMap.put(new PersonKey(i), new Person());
        }

        instance().getMap(MAP_NAME).putAll(localMap);
        createMapping(MAP_NAME, PORTABLE_FACTORY_ID, PORTABLE_KEY_ID, 0, PORTABLE_FACTORY_ID, PORTABLE_VALUE_ID, 0);
    }

    public static class PersonKey implements Portable {

        private int id;

        public PersonKey() {
            // No-op
        }

        public PersonKey(int id) {
            this.id = id;
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_KEY_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("id", id);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            id = reader.readInt("id");

            throw new IOException(ERROR_KEY);
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

            return id == personKey.id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    public static class Person implements Portable {
        public Person() {
            // No-op
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_VALUE_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) {
            // No-op
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            throw new IOException(ERROR_VALUE);
        }
    }
}
