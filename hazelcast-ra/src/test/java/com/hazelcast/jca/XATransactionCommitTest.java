package com.hazelcast.jca;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import javax.naming.InitialContext;
import javax.transaction.UserTransaction;

import org.junit.Test;

import static org.junit.Assert.*;

public class XATransactionCommitTest extends AbsDeploymentTest {

	@Test
	public void testTransactionCommi() throws Throwable {

		
		ConnectionImpl c = connectionFactory.getConnection();

context = new InitialContext();
		
		Object o = context.lookup("java:/UserTransaction");
		
		assertNotNull(o);
		UserTransaction tx = (UserTransaction) o;
		
		Map<String, String> m = c.getMap("testmap");

		tx.begin();
		
		m.put("key", "value");
		
		assertEquals("value", m.get("key"));
		
		tx.commit();
		
		assertEquals("value", m.get("key"));
		
		
	}
	
	
}
