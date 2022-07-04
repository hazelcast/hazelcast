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
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallId;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallTimeout;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        operationService = getOperationService(local);
        clusterService = getClusterService(local);
        operationRunner = new OperationRunnerImpl(operationService, getPartitionId(local), 0, newSwCounter(), null);
        responseHandler = mock(OperationResponseHandler.class);
    }

    @Test
    public void runTask() {
        final AtomicLong counter = new AtomicLong();
        operationRunner.run(() -> counter.incrementAndGet());
        assertEquals(1, counter.get());
    }

    @Test
    public void runOperation() {
        final AtomicLong counter = new AtomicLong();
        final Object response = "someresponse";
        Operation op = new Operation() {
            @Override
            public void run() {
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

        Operation op = new Operation() {
            @Override
            public void run() {
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

        Operation op = new Operation() {
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
        Operation op = new Operation() {
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
            public void run() {
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

        Operation op = new Operation() {
            @Override
            public void run() {
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

        Packet packet = toPacket(local, remote, op);
        operationRunner.run(packet);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void runPacket_whenBroken() throws Exception {
        Operation op = new DummyOperation();
        setCallId(op, 1000 * 1000);

        Packet packet = toPacket(local, remote, op);
        byte[] bytes = packet.toByteArray();
        for (int k = 0; k < bytes.length; k++) {
            bytes[k]++;
        }
        operationRunner.run(packet);
    }

    public abstract class DummyWaitingOperation extends Operation implements BlockingOperation {
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
