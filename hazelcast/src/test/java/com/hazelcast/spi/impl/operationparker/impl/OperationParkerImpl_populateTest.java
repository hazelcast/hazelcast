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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallsPerMember;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallId;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationParkerImpl_populateTest extends HazelcastTestSupport {

    @Test
    public void populateLocalCall() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
        OperationParkerImpl operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();
        Address thisAddress = nodeEngine.getThisAddress();

        DummyBlockingOperation blockingOperation = new DummyBlockingOperation(new WaitNotifyKeyImpl());
        setCallId(blockingOperation, 100);
        operationParker.park(blockingOperation);

        CallsPerMember callsPerMember = new CallsPerMember(thisAddress);
        operationParker.populate(callsPerMember);

        assertEquals(singleton(thisAddress), callsPerMember.addresses());
        assertArrayEquals(new long[]{100}, callsPerMember.toOpControl(thisAddress).runningOperations());
    }

    @Test
    public void populateRemoteCall() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();

        NodeEngineImpl nodeEngine = getNode(local).nodeEngine;
        OperationParkerImpl operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();
        Address thisAddress = getNode(local).nodeEngine.getThisAddress();
        Address thatAddress = getNode(remote).nodeEngine.getThisAddress();

        DummyBlockingOperation blockingOperation = new DummyBlockingOperation(new WaitNotifyKeyImpl());
        setCallerAddress(blockingOperation, thatAddress);
        setCallId(blockingOperation, 100);
        operationParker.park(blockingOperation);

        CallsPerMember callsPerMember = new CallsPerMember(thisAddress);
        operationParker.populate(callsPerMember);

        assertEquals(singleton(thatAddress), callsPerMember.addresses());
        assertArrayEquals(new long[]{100}, callsPerMember.toOpControl(thatAddress).runningOperations());
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
