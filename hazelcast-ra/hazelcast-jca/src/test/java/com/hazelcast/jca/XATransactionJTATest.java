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
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
//import org.jboss.arquillian.transaction.api.annotation.TransactionMode;
//import org.jboss.arquillian.transaction.api.annotation.Transactional;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Resource;
import javax.transaction.UserTransaction;

import static org.junit.Assert.assertEquals;

/**
* Arquillian tests for xa transactions
*
* @author asimarslan
*/
@RunWith(Arquillian.class)
public class XATransactionJTATest extends AbstractDeploymentTest {


    @Test
//    @Transactional(TransactionMode.ROLLBACK)
    @InSequence(1)
    public void testTransactionRollback() throws Throwable {
        HazelcastConnection c = getConnection();
        TransactionalMap<String, String> m = c.getTransactionalMap("testTransactionRollback");
        m.put("key", "value");
        assertEquals("value", m.get("key"));
        c.close();
    }

    @Test
    @InSequence(2)
    public void testTransactionRollbackPostCheck() throws Throwable {
        HazelcastConnection con2 = getConnection();
        assertEquals(null, con2.getMap("testTransactionRollback").get("key"));
        con2.close();
    }

/*    private void doSql() throws NamingException, SQLException {

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

        try {
            stmt = con.createStatement();
            stmt.execute("INSERT INTO TEST VALUES ('ab')");
            stmt.execute("SELECT * FROM TEST");
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            con.close();
        }
    }*/



}