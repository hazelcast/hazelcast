package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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

        LiveOperations liveOperations = new LiveOperations(thisAddress);
        operationParker.populate(liveOperations);

        assertEquals(singleton(thisAddress), liveOperations.addresses());
        assertArrayEquals(new long[]{100}, liveOperations.callIds(thisAddress));
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

        LiveOperations liveOperations = new LiveOperations(thisAddress);
        operationParker.populate(liveOperations);

        assertEquals(singleton(thatAddress), liveOperations.addresses());
        assertArrayEquals(new long[]{100},  liveOperations.callIds(thatAddress));
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
