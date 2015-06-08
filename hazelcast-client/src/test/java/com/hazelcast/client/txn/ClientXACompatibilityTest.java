package com.hazelcast.client.txn;

import com.atomikos.datasource.xa.XID;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientXACompatibilityTest extends HazelcastTestSupport {

    private HazelcastInstance instance, secondInstance, client, secondClient;
    private HazelcastXAResource xaResource, secondXaResource, instanceXaResource;
    private Xid xid;

    private static Xid createXid() throws InterruptedException {
        return new XID(randomString(), "test");
    }

    @Before
    public void setUp() throws Exception {
        instance = Hazelcast.newHazelcastInstance();
        instanceXaResource = instance.getXAResource();
//        secondInstance = Hazelcast.newHazelcastInstance();

        client = HazelcastClient.newHazelcastClient();
        secondClient = HazelcastClient.newHazelcastClient();
        xaResource = client.getXAResource();
        secondXaResource = secondClient.getXAResource();
        xid = createXid();
    }

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
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
            assertTrue("rollback of unknown xid gives unexpected errorCode: " + xaerr.errorCode, ((XAException.XA_RBBASE <= xaerr.errorCode) && (xaerr.errorCode <= XAException.XA_RBEND))
                    || xaerr.errorCode == XAException.XAER_NOTA);
        }
    }

    private void doSomeWorkWithXa(HazelcastXAResource xaResource) throws Exception {
        xaResource.start(xid, TMNOFLAGS);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context.getMap("map");
        map.put("key", "value");
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

}
