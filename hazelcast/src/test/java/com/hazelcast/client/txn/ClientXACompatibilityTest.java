/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.txn;

import com.atomikos.datasource.xa.XID;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.transaction.TransactionalSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.CountDownLatch;

import static javax.transaction.xa.XAResource.TMJOIN;
import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientXACompatibilityTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance instance;
    private HazelcastInstance secondInstance;
    private HazelcastInstance client;
    private HazelcastInstance secondClient;

    private HazelcastXAResource xaResource;
    private HazelcastXAResource secondXaResource;
    private HazelcastXAResource instanceXaResource;

    private Xid xid;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    private static Xid createXid() {
        return new XID(randomString(), "test");
    }

    @Before
    public void setUp() throws Exception {
        instance = hazelcastFactory.newHazelcastInstance();
        instanceXaResource = instance.getXAResource();
//        secondInstance = Hazelcast.newHazelcastInstance();

        client = hazelcastFactory.newHazelcastClient();
        secondClient = hazelcastFactory.newHazelcastClient();
        xaResource = client.getXAResource();
        secondXaResource = secondClient.getXAResource();
        xid = createXid();
    }

    @Test
    public void testRecoveryRequiresRollbackOfPreparedXidOnSecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        performRollbackWithXa(secondXaResource);
    }

    @Test
    public void testRecoveryRequiresRollbackOfPreparedXidOnInstanceXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        performRollbackWithXa(instanceXaResource);
    }

    @Test
    public void testRecoveryRequiresCommitOfPreparedXidOnSecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        performCommitWithXa(secondXaResource);
    }

    @Test
    public void testRecoveryRequiresCommitOfPreparedXidOnInstanceXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        performCommitWithXa(instanceXaResource);
    }

    @Test
    public void testRecoveryReturnsPreparedXidOnXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        assertRecoversXid(xaResource);
    }

    @Test
    public void testRecoveryReturnsPreparedXidOnSecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        assertRecoversXid(secondXaResource);
    }

    @Test
    public void testRecoveryReturnsPreparedXidOnInstanceXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        assertRecoversXid(instanceXaResource);
    }

    @Test
    public void testRecoveryRequiresRollbackOfUnknownXid() throws Exception {
        performRollbackWithXa(xaResource);
    }

    @Test
    public void testIsSameRm() throws Exception {
        assertTrue(xaResource.isSameRM(secondXaResource));
    }

    @Test
    public void testIsSameRmWithInstanceXaResource() throws Exception {
        assertTrue(xaResource.isSameRM(instanceXaResource));
    }

    @Test
    public void testRecoveryAllowedAtAnyTime() throws Exception {
        recover(xaResource);
        doSomeWorkWithXa(xaResource);
        recover(xaResource);
        performPrepareWithXa(xaResource);
        recover(xaResource);
        performCommitWithXa(xaResource);
        recover(xaResource);
    }

    private void assertRecoversXid(XAResource xaResource) throws XAException {
        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
        assertTrue("" + xids.length, xids.length == 1);
    }

    private void performCommitWithXa(XAResource xaResource) throws XAException {
        xaResource.commit(xid, false);
    }

    private void performRollbackWithXa(XAResource secondXaResource) throws XAException {
        try {
            secondXaResource.rollback(xid);
        } catch (XAException xaerr) {
            assertTrue("rollback of unknown xid gives unexpected errorCode: " + xaerr.errorCode,
                    ((XAException.XA_RBBASE <= xaerr.errorCode) && (xaerr.errorCode <= XAException.XA_RBEND))
                            || xaerr.errorCode == XAException.XAER_NOTA);
        }
    }

    private void doSomeWorkWithXa(HazelcastXAResource xaResource) throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "value");
        TransactionalQueue<Object> queue = context.getQueue("queue");
        queue.offer("item");
        TransactionalList<Object> list = context.getList("list");
        list.add("item");
        TransactionalSet<Object> set = context.getSet("set");
        set.add("item");
        TransactionalMultiMap<Object, Object> mm = context.getMultiMap("mm");
        mm.put("key", "value");
        xaResource.end(xid, TMSUCCESS);
    }

    private void performPrepareWithXa(XAResource xaResource) throws XAException {
        xaResource.prepare(xid);
    }

    private void recover(XAResource xaResource) throws XAException {
        xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testManualBeginShouldThrowException() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.beginTransaction();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testManualCommitShouldThrowException() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.commitTransaction();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testManualRollbackShouldThrowException() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.rollbackTransaction();
    }

    @Test
    public void testTransactionTimeout() throws XAException {
        boolean timeoutSet = xaResource.setTransactionTimeout(2);
        assertTrue(timeoutSet);
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "val");
        xaResource.end(xid, TMSUCCESS);

        sleepSeconds(3);

        try {
            xaResource.commit(xid, true);
            fail();
        } catch (XAException e) {
            assertEquals(XAException.XA_RBTIMEOUT, e.errorCode);
        }
    }

    @Test
    public void testRollbackWithoutPrepare() throws Exception {
        doSomeWorkWithXa(xaResource);
        performRollbackWithXa(xaResource);
    }

    @Test
    public void testRollbackWithoutPrepare_EmptyTransactionLog() throws Exception {
        xaResource.start(xid, XAResource.TMNOFLAGS);
        xaResource.end(xid, XAResource.TMSUCCESS);
        performRollbackWithXa(xaResource);
    }

    @Test
    public void testRollbackWithoutPrepare_SecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performRollbackWithXa(secondXaResource);
    }

    @Test
    public void testRollbackWithoutPrepare_SecondXAResource_EmptyTransactionLog() throws Exception {
        xaResource.start(xid, XAResource.TMNOFLAGS);
        xaResource.end(xid, XAResource.TMSUCCESS);
        performRollbackWithXa(secondXaResource);
    }

    @Test
    public void testEnd_FromDifferentThread() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "value");

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                try {
                    xaResource.end(xid, XAResource.TMFAIL);
                    latch.countDown();
                } catch (XAException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        assertOpenEventually(latch, 10);
    }

    @Test(expected = XAException.class)
    public void testStart_NoFlag_ExistingXid() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        xaResource.start(xid, TMNOFLAGS);
    }

    @Test(expected = XAException.class)
    public void testStart_JoinFlag_TransactionNotExists() throws Exception {
        xaResource.start(xid, TMJOIN);
    }

    @Test(expected = XAException.class)
    public void testStart_InvalidFlag() throws Exception {
        xaResource.start(xid, -1);
    }

    @Test(expected = XAException.class)
    public void testPrepare_TransactionNotExists() throws Exception {
        xaResource.prepare(xid);
    }

    @Test(expected = XAException.class)
    public void testCommit_OnePhase_TransactionNotExists() throws Exception {
        xaResource.commit(xid, true);
    }

    @Test(expected = XAException.class)
    public void testForget_TransactionNotExists() throws Exception {
        xaResource.forget(xid);
    }

    @Test(expected = XAException.class)
    public void testForget() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        xaResource.forget(xid);

        xaResource.commit(xid, true);
    }

    @Test
    public void testDefaultTransactionTimeout() throws Exception {
        assertEquals(120, xaResource.getTransactionTimeout());

    }

    @Test
    public void testSetTransactionTimeout() throws Exception {
        assertTrue(xaResource.setTransactionTimeout(10));
        assertEquals(10, xaResource.getTransactionTimeout());
    }

    @Test
    public void testSetTransactionTimeoutToDefault() throws Exception {
        xaResource.setTransactionTimeout(10);
        assertTrue(xaResource.setTransactionTimeout(0));
        assertEquals(120, xaResource.getTransactionTimeout());
    }

    @Test
    public void testJoin_DifferentThread() throws Exception {
        final String name = randomString();
        final String key1 = randomString();
        final String key2 = randomString();
        final String val1 = randomString();
        final String val2 = randomString();

        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap(name);
        map.put(key1, val1);
        xaResource.end(xid, TMSUCCESS);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    xaResource.start(xid, TMJOIN);
                    TransactionContext transactionContext = xaResource.getTransactionContext();
                    TransactionalMap<Object, Object> m = transactionContext.getMap(name);
                    m.put(key2, val2);
                    xaResource.end(xid, TMSUCCESS);
                } catch (XAException e) {
                    e.printStackTrace();
                }

            }
        };
        thread.start();
        thread.join();

        xaResource.commit(xid, true);

        IMap<Object, Object> m = client.getMap(name);
        assertEquals(val1, m.get(key1));
        assertEquals(val2, m.get(key2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTransactionObject_UnknownService() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.getTransactionalObject(TopicService.SERVICE_NAME, "topic");
    }

    @Test(expected = TransactionNotActiveException.class)
    public void testPrepare_AlreadyPreparedTransaction() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "val");
        xaResource.end(xid, TMSUCCESS);

        xaResource.prepare(xid);
        xaResource.prepare(xid);
    }

    @Test(expected = TransactionException.class)
    public void testCommit_OnePhase_Prepared() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "val");
        xaResource.end(xid, TMSUCCESS);

        xaResource.prepare(xid);
        xaResource.commit(xid, true);
    }

    @Test(expected = TransactionException.class)
    public void testCommit_TwoPhase_NonPrepared() throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "val");
        xaResource.end(xid, TMSUCCESS);

        xaResource.commit(xid, false);
    }
}
