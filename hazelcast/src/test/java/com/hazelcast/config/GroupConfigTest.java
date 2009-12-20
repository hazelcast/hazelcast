/**
 * 
 */
package com.hazelcast.config;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 *
 */
public class GroupConfigTest {

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#GroupConfig()}.
	 */
	@Test
	public void testGroupConfig() {
		GroupConfig groupConfig = new GroupConfig();
		assertTrue(groupConfig.getName().equals(GroupConfig.DEFAULT_GROUP_NAME));
		assertTrue(groupConfig.getPassword().equals(GroupConfig.DEFAULT_GROUP_PASSWORD));
	}

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#GroupConfig(java.lang.String)}.
	 */
	@Test
	public void testGroupConfigString() {
		GroupConfig groupConfig = new GroupConfig("abc");
		assertTrue(groupConfig.getName().equals("abc"));
		assertTrue(groupConfig.getPassword().equals(GroupConfig.DEFAULT_GROUP_PASSWORD));
	}

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#GroupConfig(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testGroupConfigStringString() {
		GroupConfig groupConfig = new GroupConfig("abc","def");
		assertTrue(groupConfig.getName().equals("abc"));
		assertTrue(groupConfig.getPassword().equals("def"));
	}

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#getName()}.
	 */
	@Test
	public void testGetName() {
		GroupConfig groupConfig = new GroupConfig();
		assertTrue(groupConfig.getName().equals(GroupConfig.DEFAULT_GROUP_NAME));
	}

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#setName(java.lang.String)}.
	 */
	@Test
	public void testSetName() {
		GroupConfig groupConfig = new GroupConfig().setName("abc");
		assertTrue(groupConfig.getName().equals("abc"));
	}

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#getPassword()}.
	 */
	@Test
	public void testGetPassword() {
		GroupConfig groupConfig = new GroupConfig();
		assertTrue(groupConfig.getPassword().equals(GroupConfig.DEFAULT_GROUP_PASSWORD));
	}

	/**
	 * Test method for {@link com.hazelcast.config.GroupConfig#setPassword(java.lang.String)}.
	 */
	@Test
	public void testSetPassword() {
		GroupConfig groupConfig = new GroupConfig().setPassword("def");
		assertTrue(groupConfig.getPassword().equals("def"));
	}

}
