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


import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.resource.cci.LocalTransaction;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Arquillian test for local transactions
 *
 * @author asimarslan
 */
@RunWith(Arquillian.class)
public class LocalTransactionTest extends AbstractDeploymentTest {

    @Test
    public void testCommitMap() throws Throwable
    {
        HazelcastConnection c = getConnection();
        final LocalTransaction localTransaction = c.getLocalTransaction();
        localTransaction.begin();

        TransactionalMap<Object, Object> m = c.getTransactionalMap("testCommitMap");

        Integer key1=1;
        String value1="value1";

        Integer key2=2;
        String value2="value2";

        m.put(key1,value1);
        m.put(key2,value2);

        localTransaction.commit();

        assertEquals(c.getMap("testCommitMap").get(key1), value1);
        assertEquals(c.getMap("testCommitMap").get(key2), value2);

        c.close();
    }


    @Test
    public void testRollbackMap() throws Throwable
    {
        HazelcastConnection c = getConnection();
        final LocalTransaction localTransaction = c.getLocalTransaction();
        localTransaction.begin();

        TransactionalMap<Integer, String> m = c.getTransactionalMap("testRollbackMap");

        Integer key1=1;
        String value1="value1";

        Integer key2=2;
        String value2="value2";

        m.put(key1,value1);
        m.put(key2,value2);

        localTransaction.rollback();

        final IMap<Integer, String> testmap = c.getMap("testRollbackMap");

        assertEquals(0,testmap.size());

        assertNull(testmap.get(key1));
        assertNull(testmap.get(key2));

        c.close();
    }
}
