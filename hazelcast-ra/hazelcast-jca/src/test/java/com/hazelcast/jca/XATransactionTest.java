/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.TransactionalMap;
import org.jboss.arquillian.junit.InSequence;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Resource;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Arquillian tests for xa transactions
 *
 * @author asimarslan
 */
//@RunWith(Arquillian.class)
@Ignore
public class XATransactionTest extends AbstractDeploymentTest {


    //    @Inject
    //    UserTransaction userTx;

    @Resource(mappedName = "java:jboss/UserTransaction")
    private UserTransaction userTx;


    @Resource(mappedName = "java:jboss/datasources/ExampleDS")
    //    @Resource(mappedName = "java:/HazelcastDS")
    protected DataSource h2Datasource;

    private static boolean isDbInit = false;

    @Before
    public void Init() throws SQLException {
        if (!isDbInit) {
            isDbInit = true;
            Connection con = h2Datasource.getConnection();
            Statement stmt = null;
            try {
                stmt = con.createStatement();
                stmt.execute("create table TEST (A varchar(3))");
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                con.close();
            }
        }
    }


    @Test
    @InSequence(2)
    @Ignore
    public void testTransactionCommit() throws Throwable {
        userTx.begin();
        HazelcastConnection c = getConnection();
        try {
            TransactionalMap<String, String> m = c.getTransactionalMap("testTransactionCommit");

            m.put("key", "value");

            doSql();

            assertEquals("value", m.get("key"));
        } finally {
            c.close();
        }
        userTx.commit();
        HazelcastConnection con2 = getConnection();

        try {
            assertEquals("value", con2.getMap("testTransactionCommit").get("key"));
            validateSQLdata(true);
        } finally {
            con2.close();
        }
    }

    @Test
    @InSequence(1)
    @Ignore
    public void testTransactionRollback() throws Throwable {
        userTx.begin();

        HazelcastConnection c = getConnection();
        try {
            TransactionalMap<String, String> m = c.getTransactionalMap("testTransactionRollback");
            m.put("key", "value");
            assertEquals("value", m.get("key"));
            doSql();
        } finally {
            c.close();
        }

        userTx.rollback();

        HazelcastConnection con2 = getConnection();

        try {
            assertEquals(null, con2.getMap("testTransactionRollback").get("key"));
            validateSQLdata(false);
        } finally {
            con2.close();
        }

    }

    private void doSql() throws NamingException, SQLException {

        Connection con = h2Datasource.getConnection();
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute("INSERT INTO TEST VALUES ('txt')");
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            con.close();
        }
    }

    private void validateSQLdata(boolean hasdata) throws NamingException, SQLException {

        Connection con = h2Datasource.getConnection();
        Statement stmt = null;

        try {
            stmt = con.createStatement();

            ResultSet resultSet = null;
            try {
                resultSet = stmt.executeQuery("SELECT * FROM TEST");

            } catch (SQLException e) {
            }
            if (hasdata) {
                assertNotNull(resultSet);
                assertTrue(resultSet.first());
                assertEquals("txt", resultSet.getString("A"));
            } else {
                assertTrue(resultSet == null || resultSet.getFetchSize() == 0);
            }

        } finally {
            if (stmt != null) {
                stmt.close();
            }
            con.close();
        }
    }


}
