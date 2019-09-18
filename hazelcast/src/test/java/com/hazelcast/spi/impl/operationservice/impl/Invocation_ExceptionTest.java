/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_ExceptionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    /**
     * When an operation indicates it returns no response, it can very well mean that it doesn't have a response yet e.g.
     * an IExecutorService.submit operation since that will rely on the completion of the task; not the operation. But if
     * an exception is thrown, then this is the response of that operation since it is very likely that a real response is
     * going to follow.
     */
    @Test
    public void whenOperationReturnsNoResponse() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, new OperationsReturnsNoResponse(), 0);
        assertCompletesEventually(f);

        expected.expect(ExpectedRuntimeException.class);
        f.join();
    }

    public class OperationsReturnsNoResponse extends Operation {
        @Override
        public void run() throws Exception {
            throw new ExpectedRuntimeException();
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }
}
