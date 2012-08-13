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

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import static org.junit.Assert.*;

public abstract class AbsXADeploymentTest extends AbsDeploymentTest {

	protected DataSource ds;

	@Override
	public void setUp() throws Throwable {
		super.setUp();
		
		embeddedJCAContainer.deploy((new File("src/test/resources/jdbc-xa.rar")).toURL());
		embeddedJCAContainer.deploy((new File("src/test/resources/h2-xa-ds.xml")).toURL());
		
		Object o = context.lookup("java:/HazelcastDS");
		assertNotNull(o);
		ds = (DataSource) o;
		
		Connection con = ds.getConnection();
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute("create table TEST (A varchar(2))");
		} finally {
			if (stmt != null) {
				stmt.close();
			}
			con.close();
		}
	}
	
}
