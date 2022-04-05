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

package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.InvocationUtil.executeLocallyWithRetry;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(InvocationUtil.class);
    }


    @Test(expected = IllegalArgumentException.class)
    public void executeLocallyWithRetryFailsWhenOperationHandlerIsSet() {
        final Operation op = new Operation() {
        };
        op.setOperationResponseHandler(new OperationResponseHandler() {
            @Override
            public void sendResponse(Operation op, Object response) {

            }
        });
        executeLocallyWithRetry(null, op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeLocallyWithRetryFailsWhenOperationDoesNotReturnResponse() {
        final Operation op = new Operation() {
            @Override
            public boolean returnsResponse() {
                return false;
            }
        };
        executeLocallyWithRetry(null, op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeLocallyWithRetryFailsWhenOperationValidatesTarget() {
        final Operation op = new Operation() {
            @Override
            public boolean validatesTarget() {
                return true;
            }
        };
        executeLocallyWithRetry(null, op);
    }

    @Test
    public void executeLocallyRetriesWhenPartitionIsMigrating() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        final int randomPartitionId = (int) (Math.random() * partitionService.getPartitionCount());
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(randomPartitionId);
        partition.setMigrating();

        final String operationResponse = "operationResponse";
        final Operation operation = new LocalOperation(operationResponse)
                .setPartitionId(randomPartitionId);
        final LocalRetryableExecution execution = executeLocallyWithRetry(nodeEngineImpl, operation);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {

                }
                partition.resetMigrating();
            }
        });

        assertTrue(execution.awaitCompletion(1, TimeUnit.MINUTES));
        assertEquals(operationResponse, execution.getResponse());
    }

    public class LocalOperation extends AbstractLocalOperation {
        private final Object operationResponse;
        private Object response;

        public LocalOperation(Object operationResponse) {
            this.operationResponse = operationResponse;
        }

        @Override
        public void run() throws Exception {
            response = operationResponse;
        }

        @Override
        public Object getResponse() {
            return response;
        }

        @Override
        public boolean validatesTarget() {
            return false;
        }
    }
}
