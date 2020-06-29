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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.BlockingExec;
import com.hazelcast.sql.impl.exec.FaultyExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

/**
 * Test for different error conditions.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlErrorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final int DATA_SET_SIZE = 100;

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testTimeout() {
        // Start two instances and fill them with data
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        populate(instance1);

        // Block batches from instance2 to instance1.
        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(instance2, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            } else {
                return exec;
            }
        });

        blocker.unblockAfter(5000L);

        // Execute query on the instance1.
        SqlException error = assertSqlException(instance1, query().setTimeoutMillis(100L));

        assertEquals(SqlErrorCode.TIMEOUT, error.getCode());
        assertEquals(instance1.getLocalEndpoint().getUuid(), error.getOriginatingMemberId());
    }

    @Test
    public void testExecutionError_local() {
        checkExecutionError(true);
    }

    @Test
    public void testExecutionError_remote() {
        checkExecutionError(false);
    }

    private void checkExecutionError(boolean local) {
        // Start two instances and fill them with data
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        populate(instance1);

        // Throw an error from local executor
        String errorMessage = "Test error";

        HazelcastInstance instance = local ? instance1 : instance2;

        setExecHook(instance, exec -> {
            if (exec instanceof MapScanExec) {
                return new FaultyExec(exec, new RuntimeException(errorMessage));
            }

            return exec;
        });

        // Execute query.
        SqlException error = assertSqlException(instance1, query());

        assertEquals(SqlErrorCode.GENERIC, error.getCode());
        assertEquals(instance.getLocalEndpoint().getUuid(), error.getOriginatingMemberId());
        assertTrue(error.getMessage().contains(errorMessage));
    }

    @Test
    public void testMapMigration() {
        // Start one instance and fill it with data
        HazelcastInstance instance1 = factory.newHazelcastInstance();

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
        SqlException error = assertSqlException(instance1, query());
        assertEquals(SqlErrorCode.PARTITION_MIGRATED, error.getCode());
    }

    @Test
    public void testMapDestroy_local() {
        checkMapDestroy(true);
    }

    @Test
    public void testMapDestroy_remote() {
        checkMapDestroy(false);
    }

    private void checkMapDestroy(boolean local) {
        // Start two instances and fill them with data
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        populate(instance1);

        // Block query execution for a while
        HazelcastInstance instance = local ? instance1 : instance2;

        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(instance, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            }

            return exec;
        });

        // Destroy map as soon as when the blocking point is reached.
        new Thread(() -> {
            try {
                blocker.awaitReached();

                instance.getMap(MAP_NAME).destroy();
            } finally {
                blocker.unblockAfter(2000);
            }
        }).start();

        // Start query
        SqlException error = assertSqlException(instance1, query());
        assertEquals(SqlErrorCode.MAP_DESTROYED, error.getCode());
    }

    @Test
    public void testMemberLeave() {
        // Start two instances and fill them with data
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        populate(instance1);

        // Block query execution on a remote member
        BlockingExec.Blocker blocker = new BlockingExec.Blocker();

        setExecHook(instance2, exec -> {
            if (exec instanceof MapScanExec) {
                return new BlockingExec(exec, blocker);
            }

            return exec;
        });

        // Stop remote member when the blocking point is reached
        new Thread(() -> {
            try {
                blocker.awaitReached();

                instance2.shutdown();
            } finally {
                blocker.unblockAfter(2000);
            }
        }).start();

        // Start query
        SqlException error = assertSqlException(instance1, query());
        assertEquals(SqlErrorCode.MEMBER_LEAVE, error.getCode());
        assertEquals(instance1.getLocalEndpoint().getUuid(), error.getOriginatingMemberId());
    }

    @Test
    public void testDataTypeMismatch() {
        // Start two instances and fill them with data
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        IMap<Long, Object> map = instance1.getMap(MAP_NAME);

        for (long i = 0; i < DATA_SET_SIZE; i++) {
            Object value = i == 0 ? Long.toString(i) : i;

            map.put(i, value);
        }

        // Execute query.
        SqlException error = assertSqlException(instance1, query());

        assertEquals(SqlErrorCode.DATA_EXCEPTION, error.getCode());
        assertEquals(
            "Failed to extract map entry value because of type mismatch "
                + "[expectedClass=java.lang.Long, actualClass=java.lang.String]",
            error.getMessage()
        );
    }

    @Test
    public void testLiteMember() {
        // Start one normal member and one local member.
        factory.newHazelcastInstance(getConfig());
        HazelcastInstance liteMember = factory.newHazelcastInstance(getConfig().setLiteMember(true));

        // Insert data
        populate(liteMember);

        // Try query from the lite member.
        SqlException error = assertSqlException(liteMember, query());
        assertEquals(SqlErrorCode.GENERIC, error.getCode());
        assertEquals("SQL queries cannot be executed on lite members", error.getMessage());
    }

    @Test
    public void testParsingError() {
        HazelcastInstance instance = factory.newHazelcastInstance();

        IMap<Long, Long> map = instance.getMap(MAP_NAME);
        map.put(1L, 1L);

        SqlException error = assertSqlException(instance, new SqlQuery("SELECT bad_field FROM " + MAP_NAME));
        assertEquals(SqlErrorCode.PARSING, error.getCode());
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Test
    public void testUserCancel() {
        HazelcastInstance instance = factory.newHazelcastInstance();

        IMap<Long, Long> map = instance.getMap(MAP_NAME);
        map.put(1L, 1L);
        map.put(2L, 2L);

        try (SqlResult res = instance.getSql().query(query().setCursorBufferSize(1))) {
            res.close();

            try {
                for (SqlRow ignore : res) {
                    // No-op.
                }

                fail("Exception is not thrown");
            } catch (SqlException e) {
                assertEquals(SqlErrorCode.CANCELLED_BY_USER, e.getCode());
            }
        }
    }

    @Nonnull
    private static SqlException assertSqlException(HazelcastInstance instance, SqlQuery query) {
        try {
            execute(instance, query);

            fail("Exception is not thrown.");

            return null;
        } catch (SqlException e) {
            System.out.println(">>> Caught expected SQL error: " + e);

            return e;
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private static int execute(HazelcastInstance instance, SqlQuery query) {
        try (SqlResult res = instance.getSql().query(query)) {
            int count = 0;

            for (SqlRow ignore : res) {
                count++;
            }

            return count;
        }
    }

    private static SqlQuery query() {
        return new SqlQuery("SELECT __key, this FROM " + MAP_NAME);
    }

    private void populate(HazelcastInstance instance) {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < DATA_SET_SIZE; i++) {
            map.put(i, i);
        }

        instance.getMap(MAP_NAME).putAll(map);
    }
}
