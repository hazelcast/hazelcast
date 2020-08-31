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

package com.hazelcast.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.client.SqlClientService;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;
import com.hazelcast.sql.impl.exec.BlockingExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test for different error conditions (client).
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
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

        res.add(new Object[] { false });
        res.add(new Object[] { true });

        return res;
    }

    @Test
    public void testTimeout_execute() {
        checkTimeout(true);
    }

    @Test
    public void testTimeout_fetch() {
        checkTimeout(true, SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE * 4);
    }

    @Test
    public void testExecutionError_fromFirstMember() {
        checkExecutionError(true, true);
    }

    @Test
    public void testExecutionError_fromSecondMember() {
        checkExecutionError(true, false);
    }

    @Test
    public void testMapMigration() {
        checkMapMigration(true);
    }

    @Test
    public void testMapDestroy_firstMember() {
        checkMapDestroy(true, true);
    }

    @Test
    public void testMapDestroy_secondMember() {
        checkMapDestroy(true, false);
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
        assertEquals(SqlErrorCode.CONNECTION_PROBLEM, error.getCode());
        assertEquals("Client must be connected to at least one data member to execute SQL queries", error.getMessage());
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
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1);

        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(instance1, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            } else {
                return exec;
            }
        });

        new Thread(() -> {
            try {
                blocker.awaitReached();

                instance1.shutdown();
            } finally {
                blocker.unblockAfter(1000);
            }
        }).start();

        HazelcastSqlException error = assertSqlException(client, query());
        assertEquals(SqlErrorCode.CONNECTION_PROBLEM, error.getCode());
    }

    @Test
    public void testMemberDisconnect_fetch() {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1, SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE + 1);

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
            assertEquals(SqlErrorCode.CONNECTION_PROBLEM, e.getCode());
        }
    }

    @Test
    public void testMemberDisconnect_close() {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1, SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE + 1);

        try {
            SqlResult result = client.getSql().execute(query());

            instance1.shutdown();

            result.close();

            fail("Should fail");
        } catch (HazelcastSqlException e) {
            assertEquals(SqlErrorCode.CONNECTION_PROBLEM, e.getCode());
        }
    }

    /**
     * Make sure that client cursors are cleared up eventually on client stop.
     */
    @Test
    public void testCursorCleanupOnClientLeave() {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        Map<Integer, Integer> localMap = new HashMap<>();
        Map<Integer, Integer> map = instance1.getMap(MAP_NAME);

        for (int i = 0; i < SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE + 1; i++) {
            localMap.put(i, i);
        }

        map.putAll(localMap);

        QueryClientStateRegistry cursorRegistry = sqlInternalService(instance1).getClientStateRegistry();

        // Create dangling cursor
        client.getSql().execute("SELECT * FROM " + MAP_NAME);
        assertEquals(1, cursorRegistry.getCursorCount());

        // Ensure that the cursor is cleared on client shutdown
        client.shutdown();
        assertTrueEventually(() -> assertEquals(0, cursorRegistry.getCursorCount()));
    }

    @Test
    public void testParameterError_serialization() {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        SqlStatement query = new SqlStatement("SELECT * FROM map").addParameter(new BadParameter(true, false));

        HazelcastSqlException error = assertSqlException(client, query);
        assertEquals(SqlErrorCode.GENERIC, error.getCode());
        assertTrue(error.getMessage().contains("Failed to serialize query parameter"));
    }

    @Test
    public void testParameterError_deserialization() {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        SqlStatement query = new SqlStatement("SELECT * FROM map").addParameter(new BadParameter(false, true));

        HazelcastSqlException error = assertSqlException(client, query);
        assertEquals(SqlErrorCode.GENERIC, error.getCode());
        assertTrue(error.getMessage().contains("Read error"));
    }

    @Test
    public void testRowError_deserialization() {
        try {
            instance1 = factory.newHazelcastInstance();
            client = newClient();

            Map<Integer, BadValue> localMap = new HashMap<>();
            IMap<Integer, BadValue> map = instance1.getMap(MAP_NAME);

            for (int i = 0; i < SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE + 1; i++) {
                localMap.put(i, new BadValue());
            }

            map.putAll(localMap);

            try (SqlResult result = client.getSql().execute("SELECT this FROM " + MAP_NAME)) {
                boolean first = true;

                for (SqlRow ignore : result) {
                    if (first) {
                        BadValue.READ_ERROR.set(true);

                        first = false;
                    }
                }

                fail("Should fail");
            } catch (HazelcastSqlException e) {
                assertEquals(SqlErrorCode.GENERIC, e.getCode());
                assertEquals(client.getLocalEndpoint().getUuid(), e.getOriginatingMemberId());
                assertTrue(e.getMessage().contains("Failed to deserialize query result value"));
            }
        } finally {
            BadValue.READ_ERROR.set(false);
        }
    }

    @Test
    public void testMissingHandler() {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        try {
            ((SqlClientService) client.getSql()).missing();

            fail("Must fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Cannot process SQL client operation due to version mismatch "
                + "(please ensure that a client and a member have the same version)"));
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
