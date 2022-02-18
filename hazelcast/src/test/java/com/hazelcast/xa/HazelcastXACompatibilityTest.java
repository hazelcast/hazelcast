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

package com.hazelcast.xa;

import com.atomikos.datasource.xa.XID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.CountDownLatch;

import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastXACompatibilityTest extends HazelcastTestSupport {

    private HazelcastXAResource xaResource;
    private HazelcastXAResource secondXaResource;
    private Xid xid;

    private static Xid createXid() {
        return new XID(randomString(), "test");
    }

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance();
        HazelcastInstance secondInstance = factory.newHazelcastInstance();
        xaResource = instance.getXAResource();
        secondXaResource = secondInstance.getXAResource();
        xid = createXid();
    }

    @Test
    public void testRecoveryRequiresRollbackOfPreparedXidOnSecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        performRollbackWithXa(secondXaResource);
    }

    @Test
    public void testRecoveryRequiresCommitOfPreparedXidOnSecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        performCommitWithXa(secondXaResource);
        assertTrueEventually(() -> assertRecoversNothing(xaResource));
        assertTrueEventually(() -> assertRecoversNothing(secondXaResource));
    }

    @Test
    public void testRecoveryReturnsPreparedXidOnSecondXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        assertRecoversXid(secondXaResource);
    }

    @Test
    public void testRecoveryReturnsPreparedXidOnXAResource() throws Exception {
        doSomeWorkWithXa(xaResource);
        performPrepareWithXa(xaResource);
        assertRecoversXid(xaResource);
    }

    @Test
    public void testRecoveryRequiresRollbackOfUnknownXid() {
        performRollbackWithXa(xaResource);
    }

    @Test
    public void testIsSameRm() throws Exception {
        assertTrue(xaResource.isSameRM(secondXaResource));
    }

    private void performCommitWithXa(XAResource xaResource) throws XAException {
        xaResource.commit(xid, false);
    }

    private void performRollbackWithXa(XAResource xaResource) {
        try {
            xaResource.rollback(xid);
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
        xaResource.end(xid, XAResource.TMSUCCESS);
    }

    private void performPrepareWithXa(XAResource xaResource) throws XAException {
        xaResource.prepare(xid);
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

    private void recover(XAResource xaResource) throws XAException {
        xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
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

    private void assertRecoversXid(XAResource xaResource) throws XAException {
        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
        assertEquals("One Xid was expected when calling recover", 1, xids.length);
    }

    private void assertRecoversNothing(XAResource xaResource) throws XAException {
        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
        assertEquals("No prepared transaction should exist", 0, xids.length);
  }

}
