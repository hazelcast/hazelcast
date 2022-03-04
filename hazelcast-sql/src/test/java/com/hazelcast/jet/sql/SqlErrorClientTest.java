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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlExecute_reservedCodec;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.client.SqlClientService;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test for different error conditions (client).
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlErrorClientTest extends SqlErrorAbstractTest {

    private static final ClientConfig CONFIG_UNISOCKET = createClientConfig(false);
    private static final ClientConfig CONFIG_SMART = createClientConfig(true);

    @Parameter
    public boolean smartRouting;

    @Override
    protected ClientConfig clientConfig() {
        return smartRouting ? CONFIG_SMART : CONFIG_UNISOCKET;
    }

    @Parameters(name = "smartRouting:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[]{false});
        res.add(new Object[]{true});

        return res;
    }

    @Test
    public void testTimeout_execute() {
        checkTimeout(true);
    }

    @Test
    public void testDataTypeMismatch() {
        checkDataTypeMismatch(true);
    }

    @Test
    public void testClientConnectedToLiteMember() {
        factory.newHazelcastInstance(getConfig().setLiteMember(true));
        client = factory.newHazelcastClient(null);

        HazelcastSqlException error = assertSqlException(client, query());
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertEquals("SQL queries cannot be executed on lite members", error.getMessage());
    }

    @Test
    public void testParsingError() {
        checkParsingError(true);
    }

    @Test
    public void testUserCancel() {
        checkUserCancel(true);
    }

    /**
     * Test proper handling of member disconnect while waiting for execute result.
     */
    @Test
    public void testMemberDisconnect_execute() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        SqlStatement streamingQuery = new SqlStatement("SELECT * FROM TABLE(GENERATE_STREAM(5000))");

        HazelcastSqlException error = assertSqlExceptionWithShutdown(client, streamingQuery);
        assertErrorCode(SqlErrorCode.CONNECTION_PROBLEM, error);
    }

    @Test
    public void testMemberDisconnect_fetch() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        createMapping(instance1, MAP_NAME, long.class, long.class);
        populate(instance1, DEFAULT_CURSOR_BUFFER_SIZE + 1);

        // Get the first row.
        boolean shutdown = true;

        try {
            for (SqlRow ignore : client.getSql().execute(query())) {
                // Shutdown the member
                if (shutdown) {
                    instance1.shutdown();

                    shutdown = false;
                }
            }

            fail("Should fail");
        } catch (HazelcastSqlException e) {
            assertErrorCode(SqlErrorCode.CONNECTION_PROBLEM, e);
        }
    }

    @Test
    public void testMemberDisconnect_close() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        createMapping(instance1, MAP_NAME, long.class, long.class);
        populate(instance1, DEFAULT_CURSOR_BUFFER_SIZE + 1);

        try {
            SqlResult result = client.getSql().execute(query());

            instance1.shutdown();

            for (SqlRow ignore : result) {
                // No-op.
            }

            result.close();

            fail("Should fail");
        } catch (HazelcastSqlException e) {
            assertErrorCode(SqlErrorCode.CONNECTION_PROBLEM, e);
        }
    }

    /**
     * Make sure that client cursors are cleared up eventually on client stop.
     */
    @Test
    public void testCursorCleanupOnClientLeave() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        Map<Integer, Integer> localMap = new HashMap<>();
        Map<Integer, Integer> map = instance1.getMap(MAP_NAME);

        for (int i = 0; i < DEFAULT_CURSOR_BUFFER_SIZE + 1; i++) {
            localMap.put(i, i);
        }

        createMapping(instance1, MAP_NAME, int.class, int.class);
        map.putAll(localMap);

        QueryClientStateRegistry cursorRegistry = sqlInternalService(instance1).getClientStateRegistry();

        // Create dangling cursor
        client.getSql().execute("SELECT * FROM " + MAP_NAME);

        assertTrueEventually(() -> assertEquals(1, cursorRegistry.getCursorCount()));

        // Ensure that the cursor is cleared on client shutdown
        client.shutdown();
        assertTrueEventually(() -> assertEquals(0, cursorRegistry.getCursorCount()));
    }

    @Test
    public void testParameterError_serialization() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        SqlStatement query = new SqlStatement("SELECT * FROM map").addParameter(new BadParameter(true, false));

        HazelcastSqlException error = assertSqlException(client, query);
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertTrue(error.getMessage().contains("Failed to serialize query parameter"));
    }

    @Test
    public void testParameterError_deserialization() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        SqlStatement query = new SqlStatement("SELECT * FROM map").addParameter(new BadParameter(false, true));

        HazelcastSqlException error = assertSqlException(client, query);
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertTrue(error.getMessage().contains("Read error"));
    }

    @Test
    public void testRowError_deserialization() {
        try {
            instance1 = newHazelcastInstance(true);
            client = newClient();

            Map<Integer, BadValue> localMap = new HashMap<>();
            IMap<Integer, BadValue> map = instance1.getMap(MAP_NAME);

            createMapping(instance1, MAP_NAME, int.class, BadValue.class);
            for (int i = 0; i < DEFAULT_CURSOR_BUFFER_SIZE + 1; i++) {
                localMap.put(i, new BadValue());
            }

            map.putAll(localMap);

            try (SqlResult result = client.getSql().execute("SELECT __key, this FROM " + MAP_NAME)) {
                Iterator<SqlRow> iterator = result.iterator();

                SqlRow firstRow = iterator.next();
                firstRow.getObject("__key");
                firstRow.getObject("this");

                BadValue.READ_ERROR.set(true);
                SqlRow secondRow = iterator.next();
                secondRow.getObject("__key");
                assertThatThrownBy(() -> secondRow.getObject("this"))
                        .isInstanceOf(HazelcastSerializationException.class)
                        .hasMessageContaining("Failed to deserialize query result value");
            }
        } finally {
            BadValue.READ_ERROR.set(false);
        }
    }

    @Test
    public void testMissingHandler() {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        try {
            ClientMessage message = SqlExecute_reservedCodec.encodeRequest(
                "SELECT * FROM table",
                Collections.emptyList(),
                100L,
                100
            );

            SqlClientService clientService = ((SqlClientService) client.getSql());

            Connection connection = clientService.getQueryConnection();
            clientService.invokeOnConnection(connection, message);

            fail("Must fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unrecognized client message received"));
        }
    }

    private static ClientConfig createClientConfig(boolean smartRouting) {
        ClientConfig config = new ClientConfig();

        config.getNetworkConfig().setSmartRouting(smartRouting);

        return config;
    }

    private static class BadValue implements DataSerializable {

        private static final ThreadLocal<Boolean> READ_ERROR = ThreadLocal.withInitial(() -> false);

        @Override
        public void writeData(ObjectDataOutput out) {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            if (READ_ERROR.get()) {
                throw new IOException("Read error");
            }
        }
    }

    @SuppressWarnings("unused")
    private static class BadParameter implements DataSerializable {
        private boolean writeError;
        private boolean readError;

        @SuppressWarnings("checkstyle:RedundantModifier")
        public BadParameter() {
            // No-op.
        }

        private BadParameter(boolean writeError, boolean readError) {
            this.writeError = writeError;
            this.readError = readError;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            if (writeError) {
                throw new IOException("Write error");
            }

            out.writeBoolean(readError);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            readError = in.readBoolean();

            if (readError) {
                throw new IOException("Read error");
            }
        }
    }
}
