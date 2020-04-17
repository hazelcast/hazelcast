/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class LoggingFlowControl implements FlowControl {

    private final QueryId queryId;
    private final int edgeId;
    private final UUID localMemberId;
    private final QueryOperationHandler operationHandler;
    private boolean setupInvoked;
    private boolean fragmentCallbackInvoked;

    private BatchAddDescriptor addDescriptor;
    private BatchRemoveDescriptor removeDescriptor;

    public LoggingFlowControl(QueryId queryId, int edgeId, UUID localMemberId, QueryOperationHandler operationHandler) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.localMemberId = localMemberId;
        this.operationHandler = operationHandler;
    }

    @Override
    public void setup(QueryId queryId, int edgeId, UUID localMemberId, QueryOperationHandler operationHandler) {
        assertEquals(this.queryId, queryId);
        assertEquals(this.edgeId, edgeId);
        assertEquals(this.localMemberId, localMemberId);
        assertSame(this.operationHandler, operationHandler);

        setupInvoked = true;
    }

    @Override
    public void onBatchAdded(UUID memberId, long size, boolean last, long remoteMemory) {
        addDescriptor = new BatchAddDescriptor(memberId, size, last, remoteMemory);
    }

    @Override
    public void onBatchRemoved(UUID memberId, long size, boolean last) {
        removeDescriptor = new BatchRemoveDescriptor(memberId, size, last);
    }

    @Override
    public void onFragmentExecutionCompleted() {
        fragmentCallbackInvoked = true;
    }

    public boolean isSetupInvoked() {
        return setupInvoked;
    }

    public boolean isFragmentCallbackInvoked() {
        return fragmentCallbackInvoked;
    }

    public BatchAddDescriptor getAddDescriptor() {
        return addDescriptor;
    }

    public BatchRemoveDescriptor getRemoveDescriptor() {
        return removeDescriptor;
    }

    public static class BatchAddDescriptor {
        private final UUID memberId;
        private final long size;
        private final boolean last;
        private final long remoteMemory;

        private BatchAddDescriptor(UUID memberId, long size, boolean last, long remoteMemory) {
            this.memberId = memberId;
            this.size = size;
            this.last = last;
            this.remoteMemory = remoteMemory;
        }

        public UUID getMemberId() {
            return memberId;
        }

        public long getSize() {
            return size;
        }

        public boolean isLast() {
            return last;
        }

        public long getRemoteMemory() {
            return remoteMemory;
        }
    }

    public static class BatchRemoveDescriptor {
        private final UUID memberId;
        private final long size;
        private final boolean last;

        private BatchRemoveDescriptor(UUID memberId, long size, boolean last) {
            this.memberId = memberId;
            this.size = size;
            this.last = last;
        }

        public UUID getMemberId() {
            return memberId;
        }

        public long getSize() {
            return size;
        }

        public boolean isLast() {
            return last;
        }
    }
}
