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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.BlockingExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Test for different error conditions.
 */
@RunWith(HazelcastSerialClassRunner.class)
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
        instance1 = newHazelcastInstance(false);
        instance2 = newHazelcastInstance(true);

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
        HazelcastSqlException error = assertSqlException(instance1, query());
        assertTrue(
                "Error code: " + error.getCode(),
                error.getCode() == SqlErrorCode.CONNECTION_PROBLEM || error.getCode() == SqlErrorCode.PARTITION_DISTRIBUTION
        );
        assertEquals(instance1.getLocalEndpoint().getUuid(), error.getOriginatingMemberId());
    }

    @Test
    public void testDataTypeMismatch() {
        checkDataTypeMismatch(false);
    }

    @Test
    public void testExecuteOnLiteMember() {
        // Start one normal member and one local member.
        newHazelcastInstance(true);
        HazelcastInstance liteMember = factory.newHazelcastInstance(getConfig().setLiteMember(true));

        // Insert data
        populate(liteMember);

        // Try query from the lite member.
        HazelcastSqlException error = assertSqlException(liteMember, query());
        assertErrorCode(SqlErrorCode.GENERIC, error);
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
