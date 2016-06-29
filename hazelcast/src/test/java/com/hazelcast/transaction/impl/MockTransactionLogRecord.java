package com.hazelcast.transaction.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockTransactionLogRecord implements TransactionLogRecord {

    //public final static ConcurrentMap<String, AtomicInteger> prepare

    private boolean failPrepare;
    private boolean failCommit;
    private boolean failRollback;

    private boolean prepareCalled;
    private boolean commitCalled;
    private boolean rollbackCalled;

    public MockTransactionLogRecord() {
    }

    public MockTransactionLogRecord failPrepare() {
        this.failPrepare = true;
        return this;
    }

    public MockTransactionLogRecord failCommit() {
        this.failCommit = true;
        return this;
    }

    public MockTransactionLogRecord failRollback() {
        this.failRollback = true;
        return this;
    }

    @Override
    public Object getKey() {
        return null;
    }

    @Override
    public Operation newPrepareOperation() {
        prepareCalled = true;
        return createOperation(failPrepare);
    }

    @Override
    public Operation newCommitOperation() {
        commitCalled = true;
        return createOperation(failCommit);
    }

    @Override
    public Operation newRollbackOperation() {
        rollbackCalled = true;
        return createOperation(failRollback);
    }

    public Operation createOperation(boolean fail) {
        return new MockOperation(fail);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(failPrepare);
        out.writeBoolean(failCommit);
        out.writeBoolean(failRollback);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        failPrepare = in.readBoolean();
        failCommit = in.readBoolean();
        failRollback = in.readBoolean();
    }

    public MockTransactionLogRecord assertCommitCalled() {
        assertTrue("commit should have been called", commitCalled);
        return this;
    }

    public MockTransactionLogRecord assertPrepareCalled() {
        assertTrue("prepare should have been called", prepareCalled);
        return this;
    }

    public MockTransactionLogRecord assertPrepareNotCalled() {
        assertFalse("prepare should have not been called", prepareCalled);
        return this;
    }

    public MockTransactionLogRecord assertCommitNotCalled() {
        assertFalse("commit should not have been called", commitCalled);
        return this;
    }

    public MockTransactionLogRecord assertRollbackNotCalled() {
        assertFalse("rollback should not have been called", rollbackCalled);
        return this;
    }

    public MockTransactionLogRecord assertRollbackCalled() {
        assertTrue("rollback should have been called", rollbackCalled);
        return this;
    }

    static class MockOperation extends Operation {
        private boolean fail;

        public MockOperation() {
        }

        public MockOperation(boolean fail) {
            setPartitionId(0);
            this.fail = fail;
        }

        @Override
        public void run() throws Exception {
            if (fail) {
                throw new TransactionException();
            }
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            out.writeBoolean(fail);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            fail = in.readBoolean();
        }
    }
}
