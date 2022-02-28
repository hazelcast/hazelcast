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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_NotifyCallTimeoutTest extends HazelcastTestSupport {

    private OperationServiceImpl operationService;
    private Node node;
    private WaitNotifyKeyImpl waitNotifyKey = new WaitNotifyKeyImpl();

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        node = getNode(hz);
        operationService = (OperationServiceImpl) getOperationService(hz);
    }

    @Test
    public void testInfiniteWaitTimeout() {
        DummyBlockingOperation op = new DummyBlockingOperation(waitNotifyKey);
        op.setPartitionId(0).setWaitTimeout(-1);

        Invocation invocation = new PartitionInvocation(
                operationService.invocationContext, op, 10, MINUTES.toSeconds(2), MINUTES.toSeconds(2),
                false, false);

        OperationAccessor.setInvocationTime(op, node.getClusterService().getClusterClock().getClusterTime());

        invocation.notifyCallTimeout();

        // now we verify if the wait timeout is still inifinite
        assertEquals(-1, op.getWaitTimeout());
    }


    @Test
    public void testTimedWait_andNearingZero() {
        DummyBlockingOperation op = new DummyBlockingOperation(waitNotifyKey);
        op.setPartitionId(0).setWaitTimeout(SECONDS.toMillis(2));

        Invocation invocation = new PartitionInvocation(
                operationService.invocationContext, op, 10,
                MINUTES.toSeconds(2), MINUTES.toSeconds(2), false, false);

        OperationAccessor.setInvocationTime(op, node.getClusterService().getClusterClock().getClusterTime());

        // we sleep longer than the wait timeout; so eventually the waitTimeout should become 0
        sleepSeconds(5);

        invocation.notifyCallTimeout();

        assertEquals(0, op.getWaitTimeout());
    }

    @Test
    public void testTimedWait() {
        DummyBlockingOperation op = new DummyBlockingOperation(waitNotifyKey);

        op.setPartitionId(0).setWaitTimeout(SECONDS.toMillis(60));

        Invocation invocation = new PartitionInvocation(
                operationService.invocationContext, op, 10, MINUTES.toSeconds(2),
                MINUTES.toSeconds(2), false, false);

        OperationAccessor.setInvocationTime(op, node.getClusterService().getClusterClock().getClusterTime());

        sleepSeconds(5);

        invocation.notifyCallTimeout();

        // we have a 60 wait timeout, if we wait 5 seconds and account for gc's etc; we should keep more than 40 seconds.
        assertTrue("op.waitTimeout " + op.getWaitTimeout() + " is too small", op.getWaitTimeout() >= SECONDS.toMillis(40));
        // we also need to verify that the wait timeout has decreased.
        assertTrue("op.waitTimeout " + op.getWaitTimeout() + " is too small", op.getWaitTimeout() <= SECONDS.toMillis(55));
    }

    private static class WaitNotifyKeyImpl implements WaitNotifyKey {
        private final String objectName = UuidUtil.newUnsecureUuidString();

        @Override
        public String getServiceName() {
            return "dummy";
        }

        @Override
        public String getObjectName() {
            return objectName;
        }
    }

    static class DummyBlockingOperation extends Operation implements BlockingOperation {
        private final WaitNotifyKey waitNotifyKey;

        private DummyBlockingOperation(WaitNotifyKey waitNotifyKey) {
            this.waitNotifyKey = waitNotifyKey;
        }

        @Override
        public void run() throws Exception {

        }

        @Override
        public WaitNotifyKey getWaitKey() {
            return waitNotifyKey;
        }

        @Override
        public boolean shouldWait() {
            return true;
        }

        @Override
        public void onWaitExpire() {
        }
    }
}
