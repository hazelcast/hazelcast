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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_OffloadedTest extends HazelcastTestSupport {

    private OperationServiceImpl localOperationService;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        Config config = new Config();
        config.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "5");

        HazelcastInstance[] cluster = instanceFactory.newInstances(config, 1);

        localOperationService = getOperationService(cluster[0]);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void whenStartThrowsException_thenExceptionPropagated() {
        InternalCompletableFuture f = localOperationService.invokeOnPartition(new OffloadingOperation(op -> new Offload(op) {
            @Override
            public void start() {
                throw new ExpectedRuntimeException();
            }
        }));

        assertCompletesEventually(f);
        f.joinInternal();
    }

    @Test
    public void whenCompletesInStart() throws Exception {
        final String response = "someresponse";
        OffloadingOperation source = new OffloadingOperation(op -> new Offload(op) {
            @Override
            public void start() {
                offloadedOperation().sendResponse("someresponse");
            }
        });

        InternalCompletableFuture<String> f = localOperationService.invokeOnPartition(source);

        assertCompletesEventually(f);
        assertEquals(response, f.get());
        // make sure the source operation isn't registered anymore
        assertFalse(localOperationService.asyncOperations.contains(source));
    }

    @Test
    public void whenCompletesEventually() throws Exception {
        final String response = "someresponse";

        InternalCompletableFuture<String> f = localOperationService.invokeOnPartition(new OffloadingOperation(op -> new Offload(op) {
            @Override
            public void start() {
                new Thread(() -> {
                    sleepSeconds(5);
                    offloadedOperation().sendResponse(response);
                }).start();
            }
        }));

        assertCompletesEventually(f);
        assertEquals(response, f.get());
    }

    @Test
    public void whenOffloaded_thenAsyncOperationRegisteredOnStart_andUnregisteredOnCompletion() {
        OffloadingOperation source = new OffloadingOperation(op -> new Offload(op) {
            @Override
            public void start() {
                // we make sure that the operation is registered
                assertTrue(localOperationService.asyncOperations.contains(offloadedOperation()));
                offloadedOperation().sendResponse("someresponse");
            }
        });

        InternalCompletableFuture<String> f = localOperationService.invokeOnPartition(source);

        assertCompletesEventually(f);
        // make sure the source operation isn't registered anymore
        assertFalse(localOperationService.asyncOperations.contains(source));
    }

    private interface OffloadFactory {
        Offload create(Operation op);
    }

    public static class OffloadingOperation extends Operation {
        private final OffloadFactory offloadFactory;

        public OffloadingOperation(OffloadFactory offloadFactory) {
            this.offloadFactory = offloadFactory;
            setPartitionId(0);
        }

        @Override
        public CallStatus call() throws Exception {
            return offloadFactory.create(this);
        }
    }
}

