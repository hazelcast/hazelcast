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

package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

/**
 * This test checks the following issue:
 * <p>
 * https://github.com/hazelcast/hazelcast/issues/1745
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class InternalPartitionServiceStackOverflowTest extends HazelcastTestSupport {

    @Test
    public void testPartitionSpecificOperation() {
        test(0);
    }

    @Test
    public void testGlobalOperation() {
        test(-1);
    }

    public void test(int partitionId) {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService opService = getNode(hz).nodeEngine.getOperationService();

        int iterations = 2000;
        final CountDownLatch latch = new CountDownLatch(iterations);

        for (int k = 0; k < iterations; k++) {
            Operation op;
            if (partitionId >= 0) {
                op = new SlowPartitionAwareSystemOperation(latch, partitionId);
            } else {
                op = new SlowPartitionUnawareSystemOperation(latch);
            }
            op.setOperationResponseHandler(OperationResponseHandlerFactory.createEmptyResponseHandler());
            opService.execute(op);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, latch.getCount());
            }
        });
    }

    public static class SlowPartitionAwareSystemOperation extends Operation
            implements UrgentSystemOperation, PartitionAwareOperation {

        private final CountDownLatch latch;

        public SlowPartitionAwareSystemOperation(CountDownLatch completedLatch, int partitionId) {
            setPartitionId(partitionId);
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(10);
            latch.countDown();
        }
    }

    public static class SlowPartitionUnawareSystemOperation extends Operation
            implements UrgentSystemOperation {

        private final CountDownLatch latch;

        public SlowPartitionUnawareSystemOperation(CountDownLatch completedLatch) {
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(10);
            latch.countDown();
        }
    }

}
