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
import com.hazelcast.sql.impl.exec.BlockingExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;

/**
 * Test for different error conditions.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlErrorTest extends SqlErrorAbstractTest {
    @Test
    public void testTimeout() {
        checkTimeout(false);
    }

    @Test
    public void testExecutionError_fromFirstMember() {
        checkExecutionError(false, true);
    }

    @Test
    public void testExecutionError_fromSecondMember() {
        checkExecutionError(false, false);
    }

    @Test
    public void testMapMigration() {
        checkMapMigration(false);
    }

    @Test
    public void testMapDestroy_firstMember() {
        checkMapDestroy(false, true);
    }

    @Test
    public void testMapDestroy_secondMember() {
        checkMapDestroy(false, false);
    }

    @Test
    public void testMemberLeave() {
        // Start two instances and fill them with data
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();

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
        assertEquals(SqlErrorCode.MEMBER_CONNECTION, error.getCode());
        assertEquals(instance1.getLocalEndpoint().getUuid(), error.getOriginatingMemberId());
    }

    @Test
    public void testDataTypeMismatch() {
        checkDataTypeMismatch(false);
    }

    @Test
    public void testExecuteOnLiteMember() {
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
        checkParsingError(false);
    }

    @Test
    public void testUserCancel() {
        checkUserCancel(false);
    }
}
