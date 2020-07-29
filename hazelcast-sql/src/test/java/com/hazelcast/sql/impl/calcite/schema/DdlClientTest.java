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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlResultType;
import com.hazelcast.sql.impl.SqlTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DdlClientTest extends SqlTestSupport {
    private static final TestHazelcastFactory FACTORY = new TestHazelcastFactory();

    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        FACTORY.newHazelcastInstance();
        client = FACTORY.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void basicDdlTest() {
        // here we only test that the result gets correctly to the client
        SqlResult result = executeQuery(client, "DROP MAPPING IF EXISTS foo");

        assertEquals(SqlResultType.VOID, result.getResultType());
        assertThrows(IllegalStateException.class, () -> result.getRowMetadata());
        assertThrows(IllegalStateException.class, () -> result.iterator());
        result.close();
    }
}
