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
