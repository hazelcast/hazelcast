package com.hazelcast.jca;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class TransactionRollbackTest extends AbsDeploymentTest {

	@Test
	public void testTransactionRollback() throws Throwable {

		ConnectionImpl c = connectionFactory.getConnection();

		c.getLocalTransaction().begin();
		
		Map<String, String> m = c.getMap("testmap");
		m.put("key", "value");
		
		assertEquals("value", m.get("key"));
		
		c.getLocalTransaction().rollback();
		
		assertEquals(null, m.get("key"));
		
	}
}
