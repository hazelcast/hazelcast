/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ExecutorConfigTest {

    @Test
    public void testGetCorePoolSize() {
        ExecutorConfig executorConfig = new ExecutorConfig();
        assertTrue(executorConfig.getCorePoolSize() == ExecutorConfig.DEFAULT_CORE_POOL_SIZE);
    }

    @Test
    public void testSetCorePoolSize() {
        ExecutorConfig executorConfig = new ExecutorConfig().setCorePoolSize(1234);
        assertTrue(executorConfig.getCorePoolSize() == 1234);
    }

    @Test
    public void testGetMaxPoolsize() {
        ExecutorConfig executorConfig = new ExecutorConfig();
        assertTrue(executorConfig.getMaxPoolSize() == ExecutorConfig.DEFAULT_MAX_POOL_SIZE);
    }

    @Test
    public void testSetMaxPoolsize() {
        ExecutorConfig executorConfig = new ExecutorConfig().setMaxPoolSize(1234);
        assertTrue(executorConfig.getMaxPoolSize() == 1234);
    }

    @Test
    public void testGetKeepAliveSeconds() {
        ExecutorConfig executorConfig = new ExecutorConfig();
        assertTrue(executorConfig.getKeepAliveSeconds() == ExecutorConfig.DEFAULT_KEEP_ALIVE_SECONDS);
    }

    @Test
    public void testSetKeepAliveSeconds() {
        ExecutorConfig executorConfig = new ExecutorConfig().setKeepAliveSeconds(1234);
        assertTrue(executorConfig.getKeepAliveSeconds() == 1234);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptNegativeCorePoolSize() {
        new ExecutorConfig().setCorePoolSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptZeroCorePoolSize() {
        new ExecutorConfig().setCorePoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptNegativeMaxPoolSize() {
        new ExecutorConfig().setMaxPoolSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptZeroMaxPoolSize() {
        new ExecutorConfig().setMaxPoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptNegativeKeepAliveSeconds() {
        new ExecutorConfig().setKeepAliveSeconds(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptZeroKeepAliveSeconds() {
        new ExecutorConfig().setKeepAliveSeconds(0);
    }

    @Test
    public void testSettingCoreAndMaxPoolSize() {
        ExecutorConfig executorConfig = new ExecutorConfig().setCorePoolSize(1234).setMaxPoolSize(5678);
        assertEquals(1234, executorConfig.getCorePoolSize());
        assertEquals(5678, executorConfig.getMaxPoolSize());
    }
}
