/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql;

import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastSqlExceptionTest {
    @Test
    public void testFields() {
        UUID memberId = UUID.randomUUID();
        int errorCode = SqlErrorCode.DATA_EXCEPTION;
        String errorMessage = "error";
        Exception cause = new IllegalArgumentException();
        String suggestion = "CREATE MAPPING...";

        HazelcastSqlException exception = new HazelcastSqlException(memberId, errorCode, errorMessage, cause, suggestion);

        assertEquals(memberId, exception.getOriginatingMemberId());
        assertEquals(errorCode, exception.getCode());
        assertEquals(errorMessage, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals(suggestion, exception.getSuggestion());
    }
}
