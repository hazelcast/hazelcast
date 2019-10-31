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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationBuilderTest extends HazelcastTestSupport {

    @Test
    public void getTargetExecutionCallback_whenNull() {
        InvocationBuilder builder = new MockInvocationBuilder(null, null, 0, null);

        assertNull(builder.getTargetExecutionCallback());
    }

    @Test
    public void getTargetExecutionCallback_whenExecutionCallbackInstance() {
        InvocationBuilder builder = new MockInvocationBuilder(null, null, 0, null);

        ExecutionCallback callback = mock(ExecutionCallback.class);
        builder.setExecutionCallback(callback);

        assertSame(callback, builder.getTargetExecutionCallback());
    }

    class MockInvocationBuilder extends InvocationBuilder {

        MockInvocationBuilder(String serviceName, Operation op, int partitionId, Address target) {
            super(serviceName, op, partitionId, target);
        }

        @Override
        public <E> InvocationFuture<E> invoke() {
            return null;
        }
    }
}
