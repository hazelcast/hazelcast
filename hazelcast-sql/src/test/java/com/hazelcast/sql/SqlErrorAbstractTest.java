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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.BlockingExec;
import com.hazelcast.sql.impl.exec.FaultyExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import org.junit.After;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class SqlErrorAbstractTest extends SqlTestSupport {
    protected static final String MAP_NAME = "map";
    private static final int DATA_SET_SIZE = 100;

    protected final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

    protected HazelcastInstance instance1;
    protected HazelcastInstance instance2;
    protected HazelcastInstance client;

    @After
    public void after() {
        instance1 = null;
        instance2 = null;
        client = null;

        factory.shutdownAll();
    }

    protected void checkTimeout(boolean useClient) {
        checkTimeout(useClient, DATA_SET_SIZE);
    }

    protected void checkTimeout(boolean useClient, int dataSetSize) {
        // Start two instances and fill them with data
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1, dataSetSize);

        // Block batches from instance2 to instance1.
        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(instance2, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            } else {
                return exec;
            }
        });

        // Execute query on the instance1.
        try {
            HazelcastSqlException error = assertSqlException(useClient ? client : instance1, query().setTimeoutMillis(100L));
            assertEquals(SqlErrorCode.TIMEOUT, error.getCode());
        } finally {
            blocker.unblock();
        }
    }

    protected void checkExecutionError(boolean useClient, boolean fromFirstMember) {
        // Start two instances and fill them with data
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1);

        // Throw an error from local executor
        String errorMessage = "Test error";

        HazelcastInstance member = fromFirstMember ? instance1 : instance2;
        HazelcastInstance target = useClient ? client : instance1;

        setExecHook(member, exec -> {
            if (exec instanceof MapScanExec) {
                return new FaultyExec(exec, new RuntimeException(errorMessage));
            }

            return exec;
        });

        // Execute query.
        HazelcastSqlException error = assertSqlException(target, query());

        assertEquals(SqlErrorCode.GENERIC, error.getCode());
        assertEquals(member.getLocalEndpoint().getUuid(), error.getOriginatingMemberId());
        assertTrue(error.getMessage().contains(errorMessage));
    }

    protected void checkMapMigration(boolean useClient) {
        // Start one instance and fill it with data
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1);

        // Block query execution for a while.
        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(instance1, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            }

            return exec;
        });

        // Start a thread that will trigger migration as soon as blocking point is reached.
        new Thread(() -> {
            try {
                blocker.awaitReached();

                factory.newHazelcastInstance();
            } finally {
                blocker.unblockAfter(2000);
            }
        }).start();

        // Start query
        HazelcastSqlException error = assertSqlException(useClient ? client : instance1, query());
        assertEquals(SqlErrorCode.PARTITION_DISTRIBUTION_CHANGED, error.getCode());
    }

    protected void checkMapDestroy(boolean useClient, boolean firstMember) {
        // Start two instances and fill them with data
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        client = newClient();

        populate(instance1);

        // Block query execution for a while
        HazelcastInstance member = firstMember ? instance1 : instance2;

        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(member, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            }

            return exec;
        });

        // Destroy map as soon as the blocking point is reached.
        new Thread(() -> {
            try {
                blocker.awaitReached();

                member.getMap(MAP_NAME).destroy();
            } finally {
                blocker.unblockAfter(2000);
            }
        }).start();

        // Start query
        HazelcastSqlException error = assertSqlException(useClient ? client : instance1, query());
        assertEquals(SqlErrorCode.MAP_DESTROYED, error.getCode());
    }

    protected void checkDataTypeMismatch(boolean useClient) {
        // Start two instances and fill them with data
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        client = newClient();

        IMap<Long, Object> map = instance1.getMap(MAP_NAME);

        for (long i = 0; i < DATA_SET_SIZE; i++) {
            Object value = i == 0 ? Long.toString(i) : i;

            map.put(i, value);
        }

        // Execute query.
        HazelcastSqlException error = assertSqlException(useClient ? client : instance1, query());

        assertEquals(SqlErrorCode.DATA_EXCEPTION, error.getCode());
        assertEquals(
            "Failed to extract map entry value because of type mismatch "
                + "[expectedClass=java.lang.Long, actualClass=java.lang.String]",
            error.getMessage()
        );
    }

    protected void checkParsingError(boolean useClient) {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        IMap<Long, Long> map = instance1.getMap(MAP_NAME);
        map.put(1L, 1L);

        HazelcastInstance target = useClient ? client : instance1;

        HazelcastSqlException error = assertSqlException(target, new SqlStatement("SELECT bad_field FROM " + MAP_NAME));
        assertEquals(SqlErrorCode.PARSING, error.getCode());
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected void checkUserCancel(boolean useClient) {
        instance1 = factory.newHazelcastInstance();
        client = newClient();

        IMap<Long, Long> map = instance1.getMap(MAP_NAME);
        map.put(1L, 1L);
        map.put(2L, 2L);

        HazelcastInstance target = useClient ? client : instance1;

        try (SqlResult res = target.getSql().execute(query().setCursorBufferSize(1))) {
            res.close();

            try {
                for (SqlRow ignore : res) {
                    // No-op.
                }

                fail("Exception is not thrown");
            } catch (HazelcastSqlException e) {
                assertEquals(SqlErrorCode.CANCELLED_BY_USER, e.getCode());
            }
        }
    }

    @Nonnull
    protected static HazelcastSqlException assertSqlException(HazelcastInstance instance, SqlStatement query) {
        try {
            execute(instance, query);

            fail("Exception is not thrown.");

            return null;
        } catch (HazelcastSqlException e) {
            System.out.println(">>> Caught expected SQL error: " + e);

            return e;
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    protected static int execute(HazelcastInstance instance, SqlStatement query) {
        try (SqlResult res = instance.getSql().execute(query)) {
            int count = 0;

            for (SqlRow ignore : res) {
                count++;
            }

            return count;
        }
    }

    protected static SqlStatement query() {
        return new SqlStatement("SELECT __key, this FROM " + MAP_NAME);
    }

    protected static void populate(HazelcastInstance instance) {
        populate(instance, DATA_SET_SIZE);
    }

    protected static void populate(HazelcastInstance instance, int size) {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < size; i++) {
            map.put(i, i);
        }

        instance.getMap(MAP_NAME).putAll(map);
    }

    protected HazelcastInstance newClient() {
        return factory.newHazelcastClient(clientConfig());
    }

    protected ClientConfig clientConfig() {
        return new ClientConfig();
    }
}
