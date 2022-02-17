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

package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WaitSetTest {

    private ILogger logger = Logger.getLogger(WaitSetTest.class);
    private ConcurrentMap<WaitNotifyKey, WaitSet> waitSetMap = new ConcurrentHashMap<WaitNotifyKey, WaitSet>();
    private Queue<WaitSetEntry> delayQueue = new LinkedBlockingQueue<WaitSetEntry>();
    private NodeEngine nodeEngine;
    private OperationService operationService;

    @Before
    public void before() {
        nodeEngine = mock(NodeEngine.class);
        operationService = mock(OperationService.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
    }

    @Test
    public void park_whenNoTimeoutSet() {
        WaitSet waitSet = newWaitSet();

        BlockedOperation op = new BlockedOperation();
        waitSet.park(op);

        assertEquals(1, waitSet.size());
        assertTrue(delayQueue.isEmpty());
    }

    private WaitSet newWaitSet() {
        return new WaitSet(logger, nodeEngine, waitSetMap, delayQueue);
    }

    @Test
    public void park_whenTimeoutSet() {
        WaitSet waitSet = newWaitSet();
        BlockedOperation op = new BlockedOperation();
        op.setWaitTimeout(100);

        waitSet.park(op);

        assertEquals(1, waitSet.size());
        WaitSetEntry entry = waitSet.find(op);
        assertTrue(entry.isValid());
        assertFalse(entry.isExpired());
        assertFalse(entry.isCancelled());

        assertEquals(1, delayQueue.size());
        assertSame(entry, delayQueue.poll());
    }

    @Test
    public void unpark_whenNoTimeoutSet() {
        WaitSet waitSet = newWaitSet();

        BlockedOperation op = new BlockedOperation();
        waitSet.park(op);

        assertEquals(1, waitSet.size());
        WaitSetEntry entry = waitSet.find(op);
        assertTrue(entry.isValid());
        assertFalse(entry.isExpired());
        assertFalse(entry.isCancelled());
    }

    @Test
    public void unpark_whenSuccess() {
        WaitSet waitSet = newWaitSet();

        BlockedOperation blockedOp = newBlockingOperationWithServiceNameAndObjectId("service1", "1");
        waitSet.park(blockedOp);

        NotifyingOperation notifyOp = newNotifyingOperationWithServiceNameAndObjectId("service1", "1");
        waitSet.unpark(notifyOp, notifyOp.getNotifiedKey());

        verify(operationService).run(blockedOp);
        // the parked operation should be removed from the waitset
        assertEquals(0, waitSet.size());
        // since it is the last parked op, the waitset should be removed from the waitSetMap
        assertEquals(0, waitSetMap.size());
    }

    @Test
    public void totalValidWaitingOperations() {
        WaitSet waitSet = newWaitSet();

        waitSet.park(new BlockedOperation());
        waitSet.park(new BlockedOperation());
        waitSet.park(new BlockedOperation());

        assertEquals(3, waitSet.totalValidWaitingOperationCount());
    }

    @Test
    public void iterator() {
        WaitSet waitSet = newWaitSet();

        BlockedOperation op1 = new BlockedOperation();
        waitSet.park(op1);
        BlockedOperation op2 = new BlockedOperation();
        waitSet.park(op2);
        BlockedOperation op3 = new BlockedOperation();
        waitSet.park(op3);

        Iterator<WaitSetEntry> it = waitSet.iterator();
        assertEquals(op1, it.next().op);
        assertEquals(op2, it.next().op);
        assertEquals(op3, it.next().op);
        assertFalse(it.hasNext());
    }

    @Test
    public void invalidateAll() {
        WaitSet waitSet = newWaitSet();

        UUID uuid = UUID.randomUUID();
        UUID anotherUuid = UUID.randomUUID();

        BlockedOperation op1 = newBlockingOperationWithCallerUuid(uuid);
        waitSet.park(op1);
        BlockedOperation op2 = newBlockingOperationWithCallerUuid(anotherUuid);
        waitSet.park(op2);
        BlockedOperation op3 = newBlockingOperationWithCallerUuid(uuid);
        waitSet.park(op3);

        waitSet.invalidateAll(uuid);

        assertValid(waitSet, op1, false);
        assertValid(waitSet, op2, true);
        assertValid(waitSet, op3, false);
    }

    private static void assertValid(WaitSet waitSet, BlockedOperation operation, boolean valid) {
        WaitSetEntry entry = waitSet.find(operation);
        assertEquals(valid, entry.valid);
    }

    @Test
    public void cancelAll() {
        WaitSet waitSet = newWaitSet();

        BlockedOperation op1 = newBlockingOperationWithServiceNameAndObjectId("service1", "1");
        waitSet.park(op1);
        BlockedOperation op2 = newBlockingOperationWithServiceNameAndObjectId("service1", "2");
        waitSet.park(op2);
        BlockedOperation op3 = newBlockingOperationWithServiceNameAndObjectId("service2", "1");
        waitSet.park(op3);

        Exception cause = new Exception();
        waitSet.cancelAll("foo", "1", cause);

        assertCancelled(waitSet, op1, cause);
        assertCancelled(waitSet, op2, null);
        assertCancelled(waitSet, op3, null);
    }

    private static void assertCancelled(WaitSet waitSet, BlockedOperation operation, Exception cause) {
        WaitSetEntry entry = waitSet.find(operation);
        assertEquals(null, entry.cancelResponse);
    }

    private static BlockedOperation newBlockingOperationWithCallerUuid(UUID callerUuid) {
        return (BlockedOperation) new BlockedOperation().setCallerUuid(callerUuid);
    }

    private BlockedOperation newBlockingOperationWithServiceNameAndObjectId(String serviceName, String objectId) {
        BlockedOperation op = new BlockedOperation();
        op.objectId = objectId;
        op.setServiceName(serviceName);
        return op;
    }

    private NotifyingOperation newNotifyingOperationWithServiceNameAndObjectId(String serviceName, String objectId) {
        NotifyingOperation op = new NotifyingOperation();
        op.objectId = objectId;
        op.setServiceName(serviceName);
        return op;
    }

    private static class BlockedOperation extends Operation implements BlockingOperation {
        private String objectId;
        private boolean hasRun;

        @Override
        public WaitNotifyKey getWaitKey() {
            return new WaitNotifyKeyImpl(getServiceName(), objectId);
        }

        @Override
        public boolean shouldWait() {
            return false;
        }

        @Override
        public void onWaitExpire() {
        }

        @Override
        public void run() throws Exception {
            hasRun = true;
        }
    }

    private static class NotifyingOperation extends Operation implements Notifier {
        private String objectId;

        @Override
        public boolean shouldNotify() {
            return true;
        }

        @Override
        public WaitNotifyKey getNotifiedKey() {
            return new WaitNotifyKeyImpl(getServiceName(), objectId);
        }

        @Override
        public void run() throws Exception {
        }
    }

    private static class WaitNotifyKeyImpl implements WaitNotifyKey {
        private final String serviceName;
        private final String objectName;

        WaitNotifyKeyImpl(String serviceName, String objectName) {
            this.serviceName = serviceName;
            this.objectName = objectName;
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public String getObjectName() {
            return objectName;
        }
    }
}
