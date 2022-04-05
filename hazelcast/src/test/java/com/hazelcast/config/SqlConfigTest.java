/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlConfigTest extends HazelcastTestSupport {
    @Test
    public void testEmpty() {
        SqlConfig config = new SqlConfig();

        assertEquals(SqlConfig.DEFAULT_STATEMENT_TIMEOUT_MILLIS, config.getStatementTimeoutMillis());
    }

    @Test
    public void testNonEmpty() {
        SqlConfig config = new SqlConfig()
                .setStatementTimeoutMillis(30L);

        assertEquals(30L, config.getStatementTimeoutMillis());
    }

    @Test
    public void testQueryTimeoutZero() {
        new SqlConfig().setStatementTimeoutMillis(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryTimeoutNegative() {
        new SqlConfig().setStatementTimeoutMillis(-1L);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(SqlConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
