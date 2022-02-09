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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Semaphore;

import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testMemberLeave() throws InterruptedException {
        // Start two instances
        instance1 = newHazelcastInstance(false);
        instance2 = newHazelcastInstance(true);

        Semaphore semaphore = new Semaphore(0);
        Thread shutdownThread = new Thread(() -> {
            try {
                semaphore.acquire(); // wait until at least one row is received
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            instance2.shutdown();
        });
        shutdownThread.start();

        // Start query
        assertThatThrownBy(() -> {
            try (SqlResult res = instance1.getSql().execute("SELECT * FROM TABLE(GENERATE_STREAM(1000))")) {
                for (SqlRow ignore : res) {
                    semaphore.release();
                }
            }
        })
                .isInstanceOf(HazelcastSqlException.class)
                .hasRootCauseInstanceOf(MemberLeftException.class);

        shutdownThread.join();
    }

    @Test
    public void testMemberLeaveDuringQueryAfterImmediateShutdown() {
        // Start two instances
        instance1 = newHazelcastInstance(false);
        instance2 = newHazelcastInstance(true);

        SqlStatement streamingQuery = new SqlStatement("SELECT * FROM TABLE(GENERATE_STREAM(1000))");

        // Start query with immediate shutdown afterwards
        HazelcastSqlException error = assertSqlExceptionWithShutdown(instance1, streamingQuery);
        assertInstanceOf(HazelcastInstanceNotActiveException.class, findRootCause(error));
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
