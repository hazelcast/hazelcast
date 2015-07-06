/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.SlowTest;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.cci.LocalTransaction;
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
@RunWith(Arquillian.class)
@Category(SlowTest.class)
public class XATransactionTest extends AbstractDeploymentTest {

    private static HazelcastConnectionFactory connectionFactory;

    private static UserTransaction userTx;

    private static DataSource h2Datasource;

    private static boolean isDbInit = false;

    @BeforeClass
    public static void init() throws SQLException, NamingException {
        InitialContext context = new InitialContext();
        userTx = (UserTransaction) context.lookup("java:comp/UserTransaction");
        connectionFactory = (HazelcastConnectionFactory) context.lookup ( "HazelcastCF" );
        if (!isDbInit) {
            isDbInit = true;

            h2Datasource = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");

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

    @Test
    public void testLocalTransaction() throws Throwable {
        HazelcastConnection c = getConnection();
        LocalTransaction lt = c.getLocalTransaction();
        lt.begin();
        try {
            TransactionalMap<String, String> m = c.getTransactionalMap("testTransactionCommit");
            m.put("key", "value");
            doSql();
            assertEquals("value", m.get("key"));
        } finally {
            c.close();
        }
        lt.commit();
        HazelcastConnection con2 = getConnection();

        try {
            assertEquals("value", con2.getMap("testTransactionCommit").get("key"));
        } finally {
            con2.close();
        }
    }

    protected HazelcastConnection getConnection() throws Throwable{
        assertNotNull(connectionFactory);
        HazelcastConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        return c;
    }

    protected void doSql() throws NamingException, SQLException {

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

    protected void validateSQLdata(boolean hasdata) throws NamingException, SQLException {

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
