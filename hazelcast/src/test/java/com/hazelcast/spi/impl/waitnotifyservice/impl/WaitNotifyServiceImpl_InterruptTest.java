package com.hazelcast.spi.impl.waitnotifyservice.impl;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.InterruptedResponse;
import com.hazelcast.spi.impl.waitnotifyservice.InternalWaitNotifyService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WaitNotifyServiceImpl_InterruptTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalWaitNotifyService waitNotifyService;
    private String callerUUID;
    private OperationExecutor executor;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        callerUUID = nodeEngineImpl.getLocalMember().getUuid();
        waitNotifyService = nodeEngineImpl.getWaitNotifyService();
        executor = ((OperationServiceImpl) getOperationService(hz)).getOperationExecutor();
    }

    @Test
    public void whenOperationNotExist_thenCallIgnored() {
        waitNotifyService.interrupt(new MockWaitNotifyKey(), callerUUID, 10);
    }

    @Test
    public void whenOperationForCallIdDoesNotExist_thenCallIgnored() {
        // first we register an operation
        BlockedOperation op = new BlockedOperation(10, callerUUID);
        OperationResponseHandler responseHandler = mock(OperationResponseHandler.class);
        op.setOperationResponseHandler(responseHandler);
        waitNotifyService.await(op);

        // then we interrupt an operation for the given waitNotifyKey/callerUUID but a different callId.
        waitNotifyService.interrupt(op.waitNotifyKey, callerUUID, op.getCallId() + 1);

        //todo: now we need to make sure the operation has not been interrupted.
    }

    @Test
    public void whenOperationNotYetInterrupted() {
        final BlockedOperation op = new BlockedOperation(10, callerUUID);
        final MockResponseHandler responseHandler = new MockResponseHandler();
        op.setOperationResponseHandler(responseHandler);
        waitNotifyService.await(op);
        waitNotifyService.interrupt(op.waitNotifyKey, callerUUID, op.getCallId());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(InterruptedResponse.class, responseHandler.response);
                // we need to make sure that the operation was executed using a partition-operation thread.
                assertInstanceOf(PartitionOperationThread.class, responseHandler.thread);
            }
        });
    }

    static class MockResponseHandler implements OperationResponseHandler {
        private volatile Object response;
        private volatile Thread thread;

        @Override
        public void sendResponse(Operation op, Object response) {
            this.response = response;
            this.thread = Thread.currentThread();
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }

    static class MockWaitNotifyKey implements WaitNotifyKey {
        private String objectName = randomString();

        @Override
        public String getServiceName() {
            return "foo";
        }

        @Override
        public String getObjectName() {
            return objectName;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(objectName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            objectName = in.readUTF();
        }
    }

    static class BlockedOperation extends AbstractOperation implements WaitSupport {
        private WaitNotifyKey waitNotifyKey;

        public BlockedOperation(long callId, String callerUuid) {
            this.waitNotifyKey = new MockWaitNotifyKey();
            OperationAccessor.setCallId(this, callId);
            setCallerUuid(callerUuid);
            setPartitionId(0);
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
            return false;
        }

        @Override
        public void onWaitExpire() {

        }
    }
}
