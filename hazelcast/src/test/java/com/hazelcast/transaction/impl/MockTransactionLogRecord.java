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

package com.hazelcast.transaction.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.internal.util.ConstructorFunction;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockTransactionLogRecord implements TransactionLogRecord {

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

        MockOperation() {
        }

        MockOperation(boolean fail) {
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

    @Override
    public int getFactoryId() {
        return MockTransactionLogRecordSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MockTransactionLogRecordSerializerHook.MOCK_TRANSACTION_LOG_RECORD;
    }

    public static class MockTransactionLogRecordSerializerHook implements DataSerializerHook {
        public static final int F_ID = 1;
        public static final int MOCK_TRANSACTION_LOG_RECORD = 0;
        public static final int LEN = MOCK_TRANSACTION_LOG_RECORD + 1;

        @Override
        public int getFactoryId() {
            return F_ID;
        }

        @Override
        public DataSerializableFactory createFactory() {
            ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

            constructors[MOCK_TRANSACTION_LOG_RECORD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                public IdentifiedDataSerializable createNew(Integer arg) {
                    return new MockTransactionLogRecord();
                }
            };

            return new ArrayDataSerializableFactory(constructors);
        }
    }
}
