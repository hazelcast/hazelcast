/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

import static org.junit.Assert.*;

import org.junit.Test;

public class ExecutorConfigTest {

	@Test
	public void testGetCorePoolSize() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		assertTrue(executorConfig.getCorePoolSize() == ExecutorConfig.DEFAULT_CORE_POOL_SIZE);
	}

	@Test
	public void testSetCorePoolSize() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setCorePoolSize(1234);
		assertTrue(executorConfig.getCorePoolSize() == 1234);
	}

	@Test
	public void testGetMaxPoolsize() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		assertTrue(executorConfig.getMaxPoolSize() == ExecutorConfig.DEFAULT_MAX_POOL_SIZE);
	}

	@Test
	public void testSetMaxPoolsize() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setMaxPoolSize(1234);
		assertTrue(executorConfig.getMaxPoolSize() == 1234);
	}

	@Test
	public void testGetKeepAliveSeconds() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		assertTrue(executorConfig.getKeepAliveSeconds() == ExecutorConfig.DEFAULT_KEEPALIVE_SECONDS);
	}

	@Test
	public void testSetKeepAliveSeconds() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setKeepAliveSeconds(1234);
		assertTrue(executorConfig.getKeepAliveSeconds() == 1234);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAcceptNegativeCorePoolSize() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setCorePoolSize(-1);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAcceptNegativeMaxPoolSize() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setMaxPoolSize(-1);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAcceptNegativeKeepAliveSeconds() {
		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setKeepAliveSeconds(-1);
	}

}
