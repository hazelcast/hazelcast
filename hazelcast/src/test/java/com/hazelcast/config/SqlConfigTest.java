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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlConfigTest {
    @Test
    public void testEmpty() {
        SqlConfig config = new SqlConfig();

        assertEquals(SqlConfig.DEFAULT_EXECUTOR_POOL_SIZE, config.getExecutorPoolSize());
        assertEquals(SqlConfig.DEFAULT_OPERATION_POOL_SIZE, config.getOperationPoolSize());
        assertEquals(SqlConfig.DEFAULT_QUERY_TIMEOUT, config.getQueryTimeout());
    }

    @Test
    public void testNonEmpty() {
        SqlConfig config = new SqlConfig()
            .setExecutorPoolSize(10)
            .setOperationPoolSize(20)
            .setQueryTimeout(30L);

        assertEquals(10, config.getExecutorPoolSize());
        assertEquals(20, config.getOperationPoolSize());
        assertEquals(30L, config.getQueryTimeout());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecutorPoolSizeZero() {
        new SqlConfig().setExecutorPoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecutorPoolSizeNegative() {
        new SqlConfig().setExecutorPoolSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOperationPoolSizeZero() {
        new SqlConfig().setOperationPoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOperationPoolSizeNegative() {
        new SqlConfig().setOperationPoolSize(-1);
    }

    @Test
    public void testQueryTimeoutZero() {
        new SqlConfig().setQueryTimeout(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryTimeoutNegative() {
        new SqlConfig().setQueryTimeout(-1L);
    }
}
