package com.hazelcast.jca;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class SimpleMapAccessTest extends AbsDeploymentTest {

	/**
	 * 
	 * Simple test to verify deployment of myresourceadapter.rar
	 * 
	 * @throws Throwable
	 *             throwable exception
	 */

	@Test
	public void testDeployment() throws Throwable {

		ConnectionImpl c = connectionFactory.getConnection();

		Map<String, String> m = c.getMap("testmap");
		
		m.put("key", "value");
		assertEquals("value", m.get("key"));
			
	}
	
}
