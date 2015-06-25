package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class OperationRunnerImplTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private HazelcastInstance remote;
    private OperationRunnerImpl operationRunner;
    private OperationServiceImpl operationService;
    private ClusterService clusterService;
    private OperationResponseHandler responseHandler;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        local = cluster[0];
        remote = cluster[1];
        operationService = (OperationServiceImpl) getOperationService(local);
        clusterService = getClusterService(local);
        operationRunner = new OperationRunnerImpl(operationService, getPartitionId(local));
        responseHandler = mock(OperationResponseHandler.class);
    }

    @Test
    public void runTask() {
        final AtomicLong counter = new AtomicLong();
        operationRunner.run(new Runnable() {
            @Override
            public void run() {
                counter.incrementAndGet();
            }
        });
        assertEquals(1, counter.get());
    }

    @Test
    public void runOperation() {
        final AtomicLong counter = new AtomicLong();
        final Object response = "someresponse";
        Operation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                counter.incrementAndGet();
            }

            @Override
            public Object getResponse() {
                return response;
            }
        };
        op.setPartitionId(operationRunner.getPartitionId());
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);
        assertEquals(1, counter.get());
        verify(responseHandler).sendResponse(op, response);
    }

    @Test
    public void runOperation_whenGeneric() {
        final AtomicLong counter = new AtomicLong();
        final Object response = "someresponse";

        Operation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                counter.incrementAndGet();
            }

            @Override
            public Object getResponse() {
                return response;
            }
        };
        op.setPartitionId(-1);
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);

        assertEquals(1, counter.get());
        verify(responseHandler).sendResponse(op, response);
    }

    @Test
    public void runOperation_whenWrongPartition_thenTaskNotExecuted() {
        final AtomicLong counter = new AtomicLong();

        Operation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                counter.incrementAndGet();
            }
        };
        op.setPartitionId(operationRunner.getPartitionId() + 1);
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);

        assertEquals(0, counter.get());
        verify(responseHandler).sendResponse(same(op), any(IllegalStateException.class));
    }

    @Test
    public void runOperation_whenRunThrowsException() {
        Operation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedRuntimeException();
            }
        };
        op.setOperationResponseHandler(responseHandler);
        op.setPartitionId(operationRunner.getPartitionId());

        operationRunner.run(op);

        verify(responseHandler).sendResponse(same(op), any(ExpectedRuntimeException.class));
    }

    @Test
    public void runOperation_whenWaitingNeeded() {
        final AtomicLong counter = new AtomicLong();

        DummyWaitingOperation op = new DummyWaitingOperation() {
            @Override
            public void run() throws Exception {
                counter.incrementAndGet();
            }
        };
        op.setPartitionId(operationRunner.getPartitionId());

        operationRunner.run(op);
        assertEquals(0, counter.get());
        // verify that the response handler was not called
        verify(responseHandler, never()).sendResponse(same(op), any());
    }

    @Test
    public void runOperation_whenTimeout_thenOperationNotExecuted() {
        final AtomicLong counter = new AtomicLong();

        Operation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                counter.incrementAndGet();
            }
        };
        setCallId(op, 10);
        setCallTimeout(op, clusterService.getClusterClock().getClusterTime() - 1);
        op.setPartitionId(operationRunner.getPartitionId());
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);
        assertEquals(0, counter.get());
        verify(responseHandler).sendResponse(same(op), any(CallTimeoutResponse.class));
    }

    @Test
    public void runPacket() throws Exception {
        Operation op = new DummyOperation();
        setCallId(op, 1000 * 1000);

        Packet packet = toPacket(remote, op);
        operationRunner.run(packet);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void runPacket_whenBroken() throws Exception {
        Operation op = new DummyOperation();
        setCallId(op, 1000 * 1000);

        Packet packet = toPacket(remote, op);
        byte[] bytes = packet.getData().toByteArray();
        for (int k = 0; k < bytes.length; k++) {
            bytes[k]++;
        }
        operationRunner.run(packet);
    }

    public abstract class DummyWaitingOperation extends AbstractOperation implements WaitSupport {
        WaitNotifyKey waitNotifyKey = new WaitNotifyKey() {
            @Override
            public String getServiceName() {
                return "someservice";
            }

            @Override
            public String getObjectName() {
                return "someobject";
            }
        };

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