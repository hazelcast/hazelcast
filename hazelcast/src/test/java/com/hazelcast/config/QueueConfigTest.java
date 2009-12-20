/**
 * 
 */
package com.hazelcast.config;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 *
 */
public class QueueConfigTest {

	/**
	 * Test method for {@link com.hazelcast.config.QueueConfig#getName()}.
	 */
	@Test
	public void testGetName() {
		QueueConfig queueConfig = new QueueConfig();
		assertNull(null, queueConfig.getName());
	}

	/**
	 * Test method for {@link com.hazelcast.config.QueueConfig#setName(java.lang.String)}.
	 */
	@Test
	public void testSetName() {
		QueueConfig queueConfig = new QueueConfig().setName("a test name");
		assertEquals("a test name", queueConfig.getName());
	}

	/**
	 * Test method for {@link com.hazelcast.config.QueueConfig#getMaxSizePerJVM()}.
	 */
	@Test
	public void testGetMaxSizePerJVM() {
		QueueConfig queueConfig = new QueueConfig();
		assertEquals(QueueConfig.DEFAULT_MAX_SIZE_PER_JVM, queueConfig.getMaxSizePerJVM());
	}

	/**
	 * Test method for {@link com.hazelcast.config.QueueConfig#setMaxSizePerJVM(int)}.
	 */
	@Test
	public void testSetMaxSizePerJVM() {
		QueueConfig queueConfig = new QueueConfig().setMaxSizePerJVM(1234);
		assertEquals(1234, queueConfig.getMaxSizePerJVM());
	}

	/**
	 * Test method for {@link com.hazelcast.config.QueueConfig#getTimeToLiveSeconds()}.
	 */
	@Test
	public void testGetTimeToLiveSeconds() {
		QueueConfig queueConfig = new QueueConfig();
		assertEquals(QueueConfig.DEFAULT_TTL_SECONDS, queueConfig.getTimeToLiveSeconds());
	}

	/**
	 * Test method for {@link com.hazelcast.config.QueueConfig#setTimeToLiveSeconds(int)}.
	 */
	@Test
	public void testSetTimeToLiveSeconds() {
		QueueConfig queueConfig = new QueueConfig().setTimeToLiveSeconds(1234);
		assertEquals(1234, queueConfig.getTimeToLiveSeconds());
	}

}
