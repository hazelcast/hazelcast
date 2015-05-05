package com.hazelcast.xa;

import com.atomikos.datasource.xa.XID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastXACompatibilityTest extends HazelcastTestSupport {

    private HazelcastInstance instance, secondInstance;
    private HazelcastXAResource xaResource, secondXaResource;
    private Xid xid;

    private static Xid createXid() throws InterruptedException {
        return new XID(randomString(), "test");
    }

    @Before
    public void setUp() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instance = factory.newHazelcastInstance();
        secondInstance = factory.newHazelcastInstance();
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

    private void assertRecoversXid(XAResource xaResource) throws XAException {
        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
        assertTrue("" + xids.length, xids.length == 1);
    }

    @Test
    public void testRecoveryRequiresRollbackOfUnknownXid() throws Exception {
        performRollbackWithXa(xaResource);
    }

    @Test
    public void testIsSameRm() throws Exception {
        assertTrue(xaResource.isSameRM(secondXaResource));
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
        xaResource.start(xid, XAResource.TMNOFLAGS);
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
        xaResource.start(xid, XAResource.TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.beginTransaction();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testManualCommitShouldThrowException() throws Exception {
        xaResource.start(xid, XAResource.TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.commitTransaction();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testManualRollbackShouldThrowException() throws Exception {
        xaResource.start(xid, XAResource.TMNOFLAGS);
        TransactionContext transactionContext = xaResource.getTransactionContext();
        transactionContext.rollbackTransaction();
    }

    @Test
    public void testCommitConcurrently() throws InterruptedException, XAException {

        int count = 10000;
        String name = randomString();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        ExecutorService executorServiceForCommit = Executors.newFixedThreadPool(5);
        for (int i = 0; i < count; i++) {
            XATransactionRunnable runnable = new XATransactionRunnable(xaResource, name, executorServiceForCommit, i);
            executorService.execute(runnable);
        }
        IMap<Object, Object> map = instance.getMap(name);
        assertSizeEventually(count, map);
    }

    private void recover(XAResource xaResource) throws XAException {
        xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    }

    static class XATransactionRunnable implements Runnable {

        HazelcastXAResource xaResource;

        String name;

        ExecutorService executorServiceForCommit;

        int i;

        public XATransactionRunnable(HazelcastXAResource xaResource, String name,
                                     ExecutorService executorServiceForCommit, int i) {
            this.xaResource = xaResource;
            this.name = name;
            this.executorServiceForCommit = executorServiceForCommit;
            this.i = i;
        }

        @Override
        public void run() {
            try {
                final Xid xid = createXid();
                xaResource.start(xid, XAResource.TMNOFLAGS);
                TransactionContext context = xaResource.getTransactionContext();
                TransactionalMap<Object, Object> map = context.getMap(name);
                map.put(i, i);
                xaResource.end(xid, XAResource.TMSUCCESS);
                executorServiceForCommit.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            xaResource.commit(xid, true);
                        } catch (XAException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
