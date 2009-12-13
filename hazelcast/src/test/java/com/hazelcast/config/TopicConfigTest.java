/**
 * 
 */
package com.hazelcast.config;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 *
 */
public class TopicConfigTest {

	/**
	 * Test method for {@link com.hazelcast.config.TopicConfig#getName()}.
	 */
	@Test
	public void testGetName() {
		TopicConfig topicConfig = new TopicConfig();
		assertNotNull(topicConfig.getName());
	}

	/**
	 * Test method for {@link com.hazelcast.config.TopicConfig#setName(java.lang.String)}.
	 */
	@Test
	public void testSetName() {
		TopicConfig topicConfig = new TopicConfig().setName("test");
		assertTrue("test".equals(topicConfig.getName()));
	}

	/**
	 * Test method for {@link com.hazelcast.config.TopicConfig#isGlobalOrderingEnabled()}.
	 */
	@Test
	public void testIsGlobalOrderingEnabled() {
		TopicConfig topicConfig = new TopicConfig();
		assertTrue(TopicConfig.DEFAULT_GLOBAL_ORDERING_ENABLED == topicConfig.isGlobalOrderingEnabled());
	}

	/**
	 * Test method for {@link com.hazelcast.config.TopicConfig#setGlobalOrderingEnabled(boolean)}.
	 */
	@Test
	public void testSetGlobalOrderingEnabled() {
		TopicConfig topicConfig = new TopicConfig().setGlobalOrderingEnabled(!TopicConfig.DEFAULT_GLOBAL_ORDERING_ENABLED);
		assertTrue(TopicConfig.DEFAULT_GLOBAL_ORDERING_ENABLED != topicConfig.isGlobalOrderingEnabled());
	}

}
