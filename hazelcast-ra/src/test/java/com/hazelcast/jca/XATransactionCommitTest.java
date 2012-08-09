/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
